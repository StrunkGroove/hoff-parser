import asyncio
import csv
import hashlib
import json
import logging
import math
import random
import string
import sys
import time
import urllib
from typing import Any, List, Union
from urllib.parse import urljoin, urlparse

import httpx
import requests
from decouple import config

from base_logic.api import AdminAPI
from base_logic.base_logic import BaseParser, ValidationGuard
from base_logic.decorators import capture_api_output
from base_logic.redis import RedisClients
from base_logic.utils import ProxyManager, Task, create_proxy, needs_update

DISABLE_SCREENSHOT = False
DEBUG = int(config("DEBUG", 0))
BASE_ADMIN_URL = config("BASE_ADMIN_URL")
BASE_SCREENSERV_URL = config("BASE_SCREENSERV_URL")

SCREENSERV_APP_ID = 150
SCREENSERV_PAGE_ID = 164
STORE_ID = 186

PROXY_MANAGER = ProxyManager(
    redis=RedisClients.get_client(), 
    type_get="random", 
    proxy_types=["socks5", "http"]
)
PARSED_LINKS = 0

sem_pages = asyncio.Semaphore(10)
sem_products = asyncio.Semaphore(10)
sem_links = asyncio.Semaphore(10)
logger = logging.getLogger("task_logger")


if DEBUG:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)


class MobileEndpoint:
    base_url = "https://hoff.ru"
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 11; Pixel Build/RQ1A.210105.003; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/83.0.4103.120 Mobile Safari/537.36",
        "Ab-Test-Data": "ab_prepayment:;ab_yandex:C;ab_sberbnpl:C;ab_delivery_notification:default;ab_save_card:;ab_android_reco_api_main_dud_app2:A;ab_android_reco_main_categories_dud_app2:A;ab_main_inspirationfeed1:A",
    }
    app_version = "8.132.0"
    salt = "6bd66d4049452e16b6e8dcdaecfe86dc"
    
    async def get_product(self, shop: dict, articul: str) -> Any:
        url = urljoin(self.base_url, "/api/v3/get_item")
        headers = self.headers.copy()
        params = {
            "articul": articul,
            "location": shop["shop_ext_id"],
            "isIOs": "1",
            "device_id": self._generate_device_id(),
            "app_version": self.app_version,
        }
        return await self._get_data(url, headers=headers, params=params)

    async def get_products(
            self, 
            shop: dict, 
            category: dict,
            limit: int = 40,
            offset: int = 0,
        ) -> List[dict]:
        url = urljoin(self.base_url, "/api/v3/get_products_new")
        headers = self.headers.copy()
        params = {
            "limit": limit,
            "sort_by": "popular",
            "sort_type": "desc",
            "offset": offset,
            "category_id": str(category["sectionId"]),
            "isIOs": "1",
            "location": shop["shop_ext_id"],
            "device_id": self._generate_device_id(),
        }
        return await self._get_data(url, headers=headers, params=params)

    async def get_categories(self, shop: dict) -> List[dict]:
        url = urljoin(self.base_url, "/api/v3/get_menu")
        headers = self.headers.copy()
        params = {
            "location": "nil",
            "isIOs": "1",
            "location": shop["shop_ext_id"],
            "device_id": self._generate_device_id(),
        }
        return await self._get_data(url, headers=headers, params=params)

    async def get_shops(self) -> List[dict]:
        url = urljoin(self.base_url, "/api/v3/location")
        headers = self.headers.copy()
        params = {
            "type": "ONLY_MAIN_CITIES",
            "query": "",
            "device_id": self._generate_device_id(),
            "isAndroid": "true",
            "isGooglePayEnabled": "0",
            "isSamsungPayEnabled": "0",
            "isAvailableSberPay": "0",
            "app_version": self.app_version,
        }
        return await self._get_data(url, headers=headers, params=params)

    @staticmethod
    def _generate_device_id() -> str:
        return "".join(random.choices(string.hexdigits.lower(), k=12))
        
    @staticmethod
    def _update_headers(url: str, headers: dict):
        parsed = urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        timestamp = int(time.time() * 1000)
        to_hash = f"{MobileEndpoint.salt}{base_url}{timestamp}"
        hash = hashlib.md5(to_hash.encode("utf-8")).hexdigest().lower()
        headers.update({
            "Timestamp": str(timestamp), 
            "Analytics": hash,
            "App-Version": MobileEndpoint.app_version,
        }) 
        return headers

    @capture_api_output
    async def _get_data(
            self, 
            url: str, 
            method="get", 
            response_type="json", 
            **kwargs
        ) -> Any:
        attempt, max_attempts = 1, 5
                
        headers = self._update_headers(url, kwargs.pop("headers"))
        
        while attempt <= max_attempts:
            try:
                proxy = PROXY_MANAGER.get_proxy()
                proxies = create_proxy(proxy, "with_delimiter")
                
                async with httpx.AsyncClient(timeout=30, proxies=proxies) as client:
                    response = await client.request(
                        method, 
                        url, 
                        headers=headers, 
                        **kwargs
                    )

                    if response_type == "json":
                        result = response.json()
                    elif response_type == "text":
                        result = response.text
                    return result
            except Exception as e:
                logger.warning(f"Attempt {attempt}: failed request — {e}")
                attempt += 1
                await asyncio.sleep(attempt * 2)

        logger.warning("Failed to get data")


async def get_sub_categories(shop: dict, category: dict) -> List[dict]:
    products_data = await MobileEndpoint().get_products(shop, category)
    if products_data.get("code"):
        return list()

    sub_categories = [
        {
            "id": int(sub_category["id"]),
            "title": {"text": sub_category["name"]},
            "sectionId": int(sub_category["id"]),
        }
        for sub_category in products_data["relatedCategories"][2:]
    ]
    return sub_categories


async def get_leaf_categories(shop: dict, tree: List[dict]) -> List[dict]:
    leaf_categories = {}

    async def traverse(node: dict, parents: List[dict]):
        current_parents = parents + [node]
        if not node.get("items"):
            sub_categories = await get_sub_categories(shop, node)
            if sub_categories:
                leaf_categories.update({
                    sub_category["id"]: {
                        **sub_category, "parents": current_parents[1:]
                    } 
                    for sub_category in sub_categories
                })
            else:
                leaf_categories[node["id"]] = {
                    **node, "parents": current_parents[1:]
                }
        else:
            for child in node["items"]:
                await traverse(child, current_parents)

    for root in tree:
        await traverse(root, [])

    return list(leaf_categories.values())


async def get_categories(
        shop: dict, 
        target_categories: Union[List[str], None]
    ) -> List[dict]:
    catalog_data = await MobileEndpoint().get_categories(shop)
    catalog = next(
        item["items"]
        for item in catalog_data["items"]
        if item["title"]["text"] == "Каталог"
    )
    if target_categories:
        catalog = [
            item 
            for item in catalog
            if str(item["id"]) in target_categories
        ]
    leaf_categories = await get_leaf_categories(shop, catalog)
    return leaf_categories


def create_screenserv_url(**kwargs) -> str:
    data = {
        "sku_id": 0,
        "app_id": SCREENSERV_APP_ID,
        "page_id": SCREENSERV_PAGE_ID,
        "data": json.dumps({**kwargs}),
    }
    screenserv_url = f"{BASE_SCREENSERV_URL}/apps/?{urllib.parse.urlencode(data)}"
    short_url = get_short_url(screenserv_url)
    return short_url


def get_short_url(full_url: str):
    url = urljoin(BASE_SCREENSERV_URL, "create-shortlink/")
    payload = {"full_url": full_url}
    response = requests.post(url, data=json.dumps(payload))
    data = response.json()
    my_hash = data["hash"]
    short_link = urljoin(BASE_SCREENSERV_URL, f"shortlink/{my_hash}")
    return short_link


class HoffParser(BaseParser):
    def save_panel_record(
            self, 
            shop: dict, 
            product: dict, 
            link: Union[dict, None] = None
        ):
        prices = product["prices"]
        price = prices.get("old") or prices.get("new")
        promo_price = prices.get("new") if prices.get("old") else 0

        tags = product.get("tag") or list()

        shortlink_data = {
            "price": int(prices.get("new")),
            "old_price": int(prices.get("old")),
            "city": shop["city"],
            "bonuses": int(product.get("bonusesForbuy", 0)),
            "reviews": product["numberOfReviews"],
            "tags": [
                {
                    "name": tag.get("text"),
                    "bg_color": tag.get("bgColor"),
                    "textColor": tag.get("textColor"),
                }
                for tag in tags
            ],
            "price_in_credit":  next(
                (
                    math.ceil(
                        (
                            int(prices.get("new"))
                            if int(prices.get("new"))
                            else int(prices.get("old"))
                        ) / 12
                    )
                    for tag in tags
                    if tag["text"] == "0-0-12"
                ),
                ""
            )
        }

        panel_record_obj = self.create_panel_record_obj(
            shop_id=int(shop["shop_row_id"]),
            product_ext_id=str(product["id"]),
            price=price,
            promo_price=promo_price,
            custom_data={
                "shop_row_id": shop["shop_row_id"],
                "address": shop["city"],
            },
            shortlink_data=shortlink_data,
        )

        if link:
            panel_record_obj.set_link_metrics(link)

        panel_record_obj.save()

        if DEBUG and not DISABLE_SCREENSHOT:
            return shortlink_data

    def save_product(
        self,
        shop: dict,
        product: dict,
        product_info: dict,
        article: str,
        link: dict = None
    ):
        categories = [product_info["categoryTitle"]]
        sizes = product_info.get("sizes", dict())
        brand = next(
            (
                item["value"]
                for item in product_info["relations"]
                if item["name"] == "Бренд"
            ),
            ""
        )

        shortlink_data = {
            "product_name": product_info["name"],
            "article": article,
            "brand": brand,
            "rating": product_info["feedback"]["rating"],
            "images": product_info["images"][:7],
            "categories": categories,
            "sizes_title": sizes.get("title", None),
            "sizes": [
                {
                    "new_price": size["newPrice"],
                    "old_price": size["oldPrice"],
                    "size": size["size"],
                    "is_available": size["isAvailable"],
                    "is_current": size["isCurrent"],
                }
                for size in sizes.get("items", list())
            ],
            "preview": (
                True
                if any((
                    product_info.get("previewAR"), 
                    product_info.get("preview360")
                ))
                else False
            ),
            "colors": [
                {   
                    "name": color["name"],
                    "image": color["image"], 
                    "is_current": color["isCurrent"]
                }
                for color in product_info.get("colors", list())
                if color["isAvailable"]
            ],
            "eyezon": True if product_info["eyezon"]["show"] else False,
            "attributes": product_info["attributes"][:3],
            "attributes_more_than_3": (
                True 
                if len(product_info["attributes"]) > 3 
                else False
            ),
            "users_photo": (
                [
                    image["photo"]
                    for image in users_photo[:6]
                ]
                if (users_photo := product_info["feedback"]["usersPhotos"])
                else None
            ),
            "total_users_photo": (
                max(len(product_info["feedback"]["usersPhotos"]) - 5, 0)
                if users_photo
                else 0
            ),
        }

        product_obj = self.create_product_obj(
            ext_id=product["id"],
            name=product_info["name"],
            url=product_info["url"],
            brand=brand,
            custom_data={
                "article": article,
                "size": next(
                    (
                        size["size"]
                        for size in sizes.get("items", list())
                        if size["isCurrent"]
                    ),
                    None
                ),
                "category_1": categories[-4] if len(categories) >= 4 else "",
                "category_2": categories[-3] if len(categories) >= 3 else "",
                "category_3": categories[-2] if len(categories) >= 2 else "",
                "category_4": categories[-1] if len(categories) >= 1 else "",
                "competitor": "hoff",
            },
            shortlink_data=shortlink_data,
        )
        product_obj.save()
        panel_record_shortlink_data = self.save_panel_record(shop, product, link)

        if DEBUG and not DISABLE_SCREENSHOT:
            objstore_url = create_screenserv_url(
                **panel_record_shortlink_data,
                **shortlink_data,
            )
            logger.debug(f"Cache page: {objstore_url}")
            logger.debug(f"Cache page: {product_info['url']}")

    async def parse_product(
            self,
            shop: dict,
            product: dict,
            product_info: dict = None,
            link: dict = None,
        ):
        try:
            if not product["in_stock"]:
                return

            _product = self.existing_products.get(str(product["id"]))
            if _product and not needs_update(_product):
                self.save_panel_record(shop, product, link)
            else:
                article = product.get("articul", product["id"])
                product_info = (
                    await MobileEndpoint().get_product(shop, article) 
                    if not product_info
                    else product_info
                )
                self.save_product(shop, product, product_info, article, link)
        except Exception:
            logger.exception(f"Cant parse product. {product['id']}")

    async def parse_page(self, shop, category, limit, offest):
        products_data = await MobileEndpoint().get_products(
            shop, category, limit, offest
        )
        tasks = [
            self.parse_product(shop, product) 
            for product in products_data["items"]
        ]
        await asyncio.gather(*tasks)
        logger.info("Parsed offset: {0}/{1}".format(
            offest + limit, products_data["totalCount"]
        ))

    async def safe_parse_page(self, shop, category, limit, offest):
        async with sem_pages:
            try:
                await self.parse_page(shop, category, limit, offest)
            except Exception:
                logger.exception(
                    f"Can't parse page. Category: {0}, offest: {1}".format(
                        category["title"]["text"], offest
                    )
                )

    async def parse_category(self, shop: dict, category: dict):
        products_data = await MobileEndpoint().get_products(shop, category)

        limit = 40
        tasks = [
            self.safe_parse_page(shop, category, limit, offest)
            for offest in range(0, products_data["totalCount"], limit)
        ]
        await asyncio.gather(*tasks)

    async def parse_link(self, shop: dict, link: dict):
        global PARSED_LINKS

        product_info = await MobileEndpoint().get_product(shop, link["url"])
        product = {
            "id": product_info["productId"],
            "in_stock": product_info["inStock"],
            "bonusesForbuy": product_info.get("bonusesForBuy", 0),
            "numberOfReviews": product_info["feedback"]["totalFeedbackCount"],
            **product_info,
        }
        await self.parse_product(
            shop, 
            product, 
            product_info=product_info, 
            link=link
        )
        PARSED_LINKS += 1
        if PARSED_LINKS % 100 == 0:
            logger.info(f"Parsed link: {PARSED_LINKS}")

    async def safe_parse_link(self, shop: dict, link: dict):
        async with sem_links:
            await self.parse_link(shop, link)
            
    async def parse_by_links(self, links: List[dict]):
        shop_id = str(links[0]["shop_row_id"])
        shop = AdminAPI.find_shop_by_shop_row_id(shop_id)
        tasks = [self.safe_parse_link(shop, link) for link in links]
        await asyncio.gather(*tasks)

    async def parse_shop(self, shop: dict, target_categories: List[str] = None):
        categories = await get_categories(shop, target_categories)
        for index, category in enumerate(categories, start=1):
            try:
                await self.parse_category(shop, category)
            except Exception as e:
                logger.exception(f"Can't parse category: {category['title']['text']}")
            else:
                logger.info(f"Parsed category: {category['title']['text']}. {index}/{len(categories)}")

    async def by_categories(self, args: dict):
        shop_id = str(args["shop_row_id"])
        shop = AdminAPI.find_shop_by_shop_row_id(shop_id)
        categories = [str(cat_id) for cat_id in args["categories"]]
        await self.parse_shop(shop, categories)

    async def panel(self, args: dict):
        shop_id = str(args["shop_row_id"])
        shop = AdminAPI.find_shop_by_shop_row_id(shop_id)
        await self.parse_shop(shop)

    async def by_links(self, args: dict):
        await self.parse_by_links(links=args["links"])

    async def parse_categories(self, args: dict):
        shop = AdminAPI.find_shop_by_shop_row_id(args["shop_row_id"])
        catalog_data = await MobileEndpoint().get_categories(shop)
        catalog = [
            {"name": cat["title"]["text"], "id": cat["id"]}
            for item in catalog_data["items"]
            if item["title"]["text"] == "Каталог"
            for cat in item["items"]
        ]
        with open("categories.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["name", "id"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(catalog)

    async def get_shops(self, args: dict):
        shops = await MobileEndpoint().get_shops()
        for shop in shops:
            shop_obj = self.create_shop_obj(
                shop_id=shop["id"],
                region= shop["region"] if shop["region"] else shop["name"],
                city=shop["name"],
                shop_data={
                    "kladrId": shop["kladrId"],
                    "businessUnitId": shop["businessUnitId"],
                },
            )
            shop_obj.save()


def parse(_task, args):
    global DISABLE_SCREENSHOT
    global SITE_ENDPOINT

    DISABLE_SCREENSHOT = args.get("disable_screenshot", False)
    SITE_ENDPOINT = MobileEndpoint()

    hoff = HoffParser(
        _task,
        args,
        store_id=STORE_ID,
        screenserv_app_id=SCREENSERV_APP_ID,
        screenserv_page_id=SCREENSERV_PAGE_ID,
        product_validation_guard=ValidationGuard(),
        panel_record_validation_guard=ValidationGuard(),
    )
    hoff.run()
    return {"result": True}


if DEBUG:
    # parse(Task(), {"parsing_type": "get_shops"})
    # parse(Task(), {"shop_row_id": 274064, "parsing_type": "parse_categories"})
    # parse(
    #     Task(),
    #     {
    #         "links": [
    #             {
    #                 "url": "7453516",
    #                 "article_id": 273965,
    #                 "shop_row_id": 274064,
    #                 "article_metro": 273965,
    #             }
    #             for _ in range(100)
    #         ],
    #         "parsing_type": "by_links",
    #         "disable_screenshot": False,
    #     },
    # )
    parse(
        Task(),
        {
            "shop_row_id": 274053, 
            "parsing_type": "by_categories", 
            "disable_screenshot": True,
            "categories": ["8375"]
        }
    )
