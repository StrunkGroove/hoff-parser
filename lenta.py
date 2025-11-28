import hashlib
import json
import logging
import random
import re
import sys
import time
import urllib.parse
import uuid
from typing import Dict, List, NoReturn, Tuple, Union
from urllib.parse import urljoin
import aiohttp
import asyncio

import requests
from decouple import config

from base_logic.api import AdminAPI
from base_logic.base_logic import BaseParser, ValidationGuard
from base_logic.decorators import capture_api_output
from base_logic.exceptions import ParserStopException
from base_logic.redis import RedisClients
from base_logic.utils import ProxyManager, Task, needs_update

DEBUG = int(config("DEBUG", 0))
DISABLE_SCREENSHOT = False

TASK_ID = None
PARSING_ID = None
STORE_ID = 25

SCREENSERV_APP_ID = 16
SCREENSERV_PAGE_ID = 17
PRODUCT_STRUCTURE_ID = 11
PANEL_RECORD_STRUCTURE_ID = 12

CREATED_PRODUCTS = 0
UPDATED_PRODUCTS = 0

BASE_URL = config("BASE_ADMIN_URL")
BASE_SCREENSERV_URL = config("BASE_SCREENSERV_URL")

SITE_URL = "https://lenta.com"
SESSION_TOKEN: str = None

shop_data_cache = {}

sem = asyncio.Semaphore(5)

PROXY_MANAGER = ProxyManager(
    redis=RedisClients.get_client(), type_get="random", proxy_types=["http_foreign"]
)


logger = logging.getLogger("task_logger")


if not DEBUG:
    from main.models import Shop
else:
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)


class LentaHeaders:
    @staticmethod
    def session_token(length: int = 32) -> str:
        return "".join(random.choices("0123456789ABCDEF", k=length))

    @staticmethod
    def device_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def user_agent() -> str:
        agents = [
            "okhttp/4.9.1",
            "LentaApp/6.8.0 (Android 11; SM-G975F Build/RP1A.200720.012)",
            "Dalvik/2.1.0 (Linux; U; Android 10; SM-G980F Build/QP1A.190711.020)",
            "Mozilla/5.0 (Android 12; Mobile; rv:91.0) Gecko/91.0 Firefox/91.0",
            "LentaAndroid/6.8.0 (Android 13; Pixel 6 Build/TQ1A.230205.002)",
            "okhttp/3.12.1",
            "Dalvik/2.1.0 (Linux; U; Android 9; SM-G960F Build/PPR1.180610.011)",
            "Mozilla/5.0 (Linux; Android 8.0.0; SM-G955U Build/R16NW) AppleWebKit/537.36",
        ]
        return random.choice(agents)


class LentaAPI:
    base_url = "https://api.lenta.com"
    headers = {
        "X-Retail-Brand": "lo",
        "X-Platform": "omniapp",
    }

    @classmethod
    async def get_shops(cls):
        url = urljoin(cls.base_url, "/v1/stores/pickup/search")
        data = await get_data(url, "POST", headers=cls.headers, json={})
        return data["items"]

    @classmethod
    async def get_categories(cls, shop: dict) -> List[dict]:
        url = urljoin(cls.base_url, "/v1/catalog/categories")
        headers = cls._get_headers(shop)
        data = await get_data(url, headers=headers)
        return data["categories"]

    @classmethod
    async def get_products(
        cls, shop: dict, category: dict, offset: int
    ) -> Tuple[int, List[dict]]:
        url = urljoin(cls.base_url, "/v1/catalog/items")
        headers = cls._get_headers(shop)
        payload = {
            "categoryId": category["id"],
            "limit": 200,
            "offset": offset,
            "sort": {"type": "popular", "order": "desc"},
            "filters": {"range": [], "checkbox": [], "multicheckbox": []},
        }
        data = await get_data(url, "POST", headers=headers, json=payload)
        return data["total"], data["items"]

    @classmethod
    async def get_product(cls, shop: dict, product_id: Union[str, int]) -> dict:
        url = urljoin(cls.base_url, f"/v1/catalog/items/{product_id}")
        headers = cls._get_headers(shop)
        data = await get_data(url, headers=headers)
        return data

    @classmethod
    async def user_adress_add(cls, shop: dict) -> dict:
        url = urljoin(cls.base_url, "/v1/users/address")
        headers = cls.headers.copy()
        headers.update(
            {"X-Device-Os": "Android", "X-Delivery-Mode": shop["delivery_type"]}
        )
        if shop.get("type", None) and shop["type"] == "hypermarket":
            zone_type = "common"
        else:
            zone_type = ""
        payload = {
            "TownTitle": shop["city"],
            "HouseNumber": str(shop["house_number"]),
            "YandexLatitude": str(shop["latitude"]),
            "YandexLongitude": str(shop["longitude"]),
            "ZoneType": zone_type,
            "StreetTitle": shop["street_title"],
            "EntranceNumber": "",
            "Information": "",
            "IsVacationHome": True,
            "Selected": False,
            "Editable": False,
        }
        data = await get_data(url, "POST", headers=headers, json=payload)
        return data

    @classmethod
    async def user_adress_get(cls, shop: dict) -> dict:
        url = urljoin(cls.base_url, "/v1/users/address")
        headers = cls.headers.copy()
        headers.update(
            {
                "X-Device-Os": "Android",
                "X-Delivery-Mode": shop["delivery_type"],
            }
        )
        params = {"offset": 0}
        data = await get_data(url, headers=headers, params=params)
        return data

    @classmethod
    async def get_session_token(cls, shop: dict) -> dict:
        url = urljoin(cls.base_url, "/v1/auth/session/guest/token")
        headers = cls._get_headers(shop)
        data = await get_data(url, headers=headers)
        return data

    @classmethod
    def _get_headers(cls, shop: dict) -> dict:
        headers = cls.headers
        headers["X-Delivery-Mode"] = shop["delivery_type"]
        if organization_id := shop.get("organization_id"):
            headers["X-Organization-Id"] = organization_id
        return headers


class LentochkaAPI:
    """Такие запросы искать только в приложении"""

    base_url = "https://lentochka.lenta.com"
    headers = {
        "Deviceid": LentaHeaders.device_id(),
        "Sessiontoken": None,
    }

    @classmethod
    async def set_location(cls, shop: dict):
        global SESSION_TOKEN
        if shop.get("session_token"):
            SESSION_TOKEN = shop["session_token"]
        else:
            session_data = await LentaAPI.get_session_token(shop)
            SESSION_TOKEN = session_data["sessionId"]

        cls.headers["Sessiontoken"] = SESSION_TOKEN

        url = urljoin(cls.base_url, "/jrpc/deliveryModeSet")
        payload = {
            "jsonrpc": "2.0",
            "method": "deliveryModeSet",
        }
        if shop["delivery_type"] == "pickup":
            payload["params"] = {
                "type": shop["delivery_type"],
                "storeId": shop["shop_id"],
            }
        elif shop["delivery_type"] == "delivery":
            address_info = await LentaAPI.user_adress_add(shop)
            payload["params"] = {
                "type": shop["delivery_type"],
                "addressId": address_info["Id"],
            }
        await get_data(url, "POST", headers=cls.headers, json=payload)

    @classmethod
    async def get_location(cls) -> dict:
        cls.headers["Sessiontoken"] = SESSION_TOKEN

        url = urljoin(cls.base_url, "/jrpc/deliveryModeGet")
        payload = {"jsonrpc": "2.0", "method": "deliveryModeGet"}
        data = await get_data(url, "POST", headers=cls.headers, json=payload)
        return data


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


def update_headers(headers: dict, url: str):
    timestamp = str(int(time.time()))
    string = "69c435bb1171fe10a4416ca2bd5ca20d" + url + timestamp
    result = hashlib.md5(string.encode())
    headers.update(
        {
            "Sessiontoken": SESSION_TOKEN if SESSION_TOKEN else "",
            "Qrator-Token": result.hexdigest(),
            "Timestamp": timestamp,
            "App-Version": "6.8.0",
            "User-Agent": LentaHeaders.user_agent(),
            "Deviceid": LentaHeaders.device_id(),
        }
    )


@capture_api_output
async def get_data(url, method="get", response_type="json", **kwargs):
    attempts = 0
    while attempts < 10:
        try:
            update_headers(kwargs.get("headers"), url)
            proxy = PROXY_MANAGER.get_proxy()
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.request(
                    method, url, timeout=15, proxy=f"http://{proxy}", **kwargs
                ) as response:
                    if response.status == 429:
                        await asyncio.sleep(1)
                        raise Exception("To many requests: status 429")
                    if response_type == "json":
                        text = await response.text()
                        result = json.loads(text)
                    else:
                        result = await response.text()
                    return result
        except Exception as e:
            attempts += 1
            logger.info(f"Can't get data, retry {attempts}. Error: {e}")
            await asyncio.sleep(attempts * 2)
    logger.warning("Failed to get data")


def set_categories(categories: List[str]) -> dict:
    new_categories = {}
    for i in range(4):
        new_categories[f"category_{4 - i}"] = (
            categories[i] if i < len(categories) else ""
        )
    return new_categories


def build_category_tree(categories: List[dict]) -> List[dict]:
    category_tree = {}

    for category in categories:
        category["children"] = []
        category_tree[category["id"]] = category

    root_categories = []
    for category in categories:
        parent_id = category["parentId"]
        if parent_id == 0:
            root_categories.append(category)
        else:
            parent = category_tree.get(parent_id)
            if parent:
                parent["children"].append(category)
    return root_categories


def del_some_categories(tree: List[dict]) -> None:
    del_categories = {
        1893: "Особенно выгодно",
        19305: "Новинки",
        18427: "Товары до 99 рублей",
        18359: "Только в Ленте",
    }
    tree[:] = [category for category in tree if category["id"] not in del_categories]


def add_parent_names(categories: List[dict], parent_names: List[str] = None) -> None:
    if parent_names is None:
        parent_names = []

    for category in categories:
        category["parent_names"] = parent_names.copy()

        add_parent_names(category["children"], parent_names + [category["name"]])


def find_leaf_categories(tree: List[dict]) -> List[dict]:
    leaf_categories = list()

    def traverse(categories):
        for category in categories:
            if not category["children"]:
                leaf_categories.append(category)
            else:
                traverse(category["children"])

    traverse(tree)
    return leaf_categories


def get_short_url(full_url: str):
    url = urljoin(BASE_SCREENSERV_URL, "create-shortlink/")
    payload = {"full_url": full_url}
    response = requests.post(url, data=json.dumps(payload))
    data = response.json()
    my_hash = data["hash"]
    short_link = urljoin(BASE_SCREENSERV_URL, f"shortlink/{my_hash}")
    return short_link


def find_by_shop_row_id(shop_row_id: Union[int, str]) -> Dict:
    global shop_data_cache
    if shop_row_id not in shop_data_cache:
        shop = requests.get(f"https://parsing.ma.works/shops/{shop_row_id}/").json()
        shop.update(shop.pop("shop_data"))
        shop["shop_row_id"] = shop_row_id
        try:
            shop["shop_id"] = int(shop["shop_ext_id"])
        except:
            pass
        shop_data_cache[shop_row_id] = shop
    return shop_data_cache[shop_row_id]


def group_by_shop(links: list) -> dict:
    """
    На выходе получается dict, сгруппированный по shop_id, вида:
    {"424228":[links]}, "424123":[links]}
    """
    result = {}
    for link in links:
        link["product_id"] = (
            urllib.parse.urlparse(link["url"]).path.split("-")[-1].replace("/", "")
        )
        shop_id = link["shop_row_id"]
        if shop_id in result.keys():
            result[shop_id].append(link)
        else:
            result.update({shop_id: [link]})
    return result


async def set_location(shop) -> Union[None, NoReturn]:
    await LentochkaAPI.set_location(shop)
    location = await LentochkaAPI.get_location()
    assert location["result"]["sessionToken"] == SESSION_TOKEN
    assert location["result"]["type"] == shop["delivery_type"]
    if shop["delivery_type"] == "delivery":
        address_info = (await LentaAPI.user_adress_get(shop))["UserAddressList"][0]
        assert address_info["TownTitle"] == shop["city"]
        assert address_info["HouseNumber"] == str(shop["house_number"])
        assert f"{float(address_info['YandexLatitude']):.6f}" == f"{float(shop['latitude']):.6f}"
        assert f"{float(address_info['YandexLongitude']):.6f}" == f"{float(shop['longitude']):.6f}"
        assert address_info["StreetTitle"] == shop["street_title"]
    elif shop["delivery_type"] == "pickup":
        assert location["result"]["storeId"] == int(shop["shop_id"])


async def get_categories(shop: dict) -> List[dict]:
    categories = await LentaAPI.get_categories(shop)
    tree = build_category_tree(categories)
    del_some_categories(tree)
    add_parent_names(tree)
    leaf_categorie = find_leaf_categories(tree)
    return leaf_categorie


async def get_all_categories(shop: dict) -> List[dict]:
    categories = await LentaAPI.get_categories(shop)
    tree = build_category_tree(categories)
    del_some_categories(tree)
    add_parent_names(tree)
    all_categories = []

    def get_all_cats(categories):
        for category in categories:
            all_categories.append(category)
            if category.get("children"):
                get_all_cats(category["children"])

    get_all_cats(tree)
    return all_categories


def extract_product_id(string: str) -> Union[str, None]:
    """
    String may was like:
    400583
    https://lenta.com/product/503139-rossiya-503139/
    https://lenta.com/product/goroshek-zelenyjj-bonduelle-rossiya-425ml-000937/
    https://lenta.com/product/myasnye-konservy-govyadina-grodfud-tushenaya-975-myasa-klyuch-belarus-338g-376161/
    """
    if "online" in string:
        logger.warning(f"Cant parse lenta online: {string}")
        return None
    pattern = r"(\d{5,6})\/?"
    match = re.search(pattern, string)
    item_id = match.group(1)
    return item_id


async def create_shops():
    shops_type_mapping = {
        "HM": "Гипер",
        "SM": "Супер",
        "EC": "Эконом",
        "ZO": "Зоо",
        "FA": "Семья",
        "AL": "Вингараж",
    }
    shops = await LentaAPI.get_shops()
    for shop in shops:
        item = {
            "store": STORE_ID,
            "shop_id": shop["id"],
            "city": shop["addressFull"].split(",")[0],
            "address": shop["addressFull"],
            "latitude": round(shop["coordinates"]["latitude"], 6),
            "longitude": round(shop["coordinates"]["longitude"], 6),
            "delivery_type": "pickup",
            "shop_type": shops_type_mapping[shop["marketType"]],
            "shop_data": {},
        }
        logger.debug(item)
        if not DEBUG:
            Shop.create_from_script(item)


class LentaParser(BaseParser):

    async def save_panel_record(self, shop: dict, product: dict, link: dict = None):
        def split_price(price):
            if not price:
                return 0, 0
            parts = str(price).split(".")
            return parts[0], parts[1]

        price = float(product["prices"]["costRegular"] / 100)
        promo_price = float(
            promo_price
            if price != (promo_price := product["prices"]["cost"] / 100)
            and product["prices"]["isPromoactionPrice"]
            else 0
        )
        card_price = float(
            card_price
            if price != (card_price := product["prices"]["cost"] / 100)
            and product["prices"]["isLoyaltyCardPrice"]
            and not product["prices"]["isPromoactionPrice"]
            else 0
        )
        splited_price = split_price(price)[0]
        splited_price_kop = split_price(price)[1]
        splited_promo_price = split_price(promo_price)[0]
        splited_promo_price_kop = split_price(price)[1]

        promo_percent = (
            round(price - promo_price) / price * 100 if promo_price else None
        )

        panel_record_obj = self.create_panel_record_obj(
            shop_id=int(shop["shop_row_id"]),
            product_ext_id=str(product["id"]),
            price=price,
            promo_price=promo_price,
            card_price=card_price,
            custom_data={
                "address": shop.get("address"),
                "shop_id": str(shop.get("shop_id")),
                "shop_row_id": int(shop["shop_row_id"]),
                "store_type": shop.get("shop_type"),
            },
            shortlink_data={
                "price": splited_price,
                "price_kop": splited_price_kop,
                "promo_price": splited_promo_price,
                "promo_price_kop": splited_promo_price_kop,
                "card_price": (
                    card_price
                    if price != (card_price := product["prices"]["cost"] / 100)
                    and product["prices"]["isLoyaltyCardPrice"]
                    and not product["prices"]["isPromoactionPrice"]
                    else 0
                ),
                "promo_percent": promo_percent,
                "address": shop.get("address"),
                "delivery_type": shop["delivery_type"],
            },
        )

        if link:
            panel_record_obj.set_link_metrics(link)
        panel_record_obj.save()

    async def save_product(
        self, shop: dict, categories: List[str], product: dict, link: dict = None
    ):
        product_obj = self.create_product_obj(
            ext_id=product["id"],
            name=product["name"],
            url=urljoin(SITE_URL, f"/product/{product['id']}"),
            custom_data={
                **set_categories(categories),
                "competitor": "lenta",
            },
            shortlink_data={
                "title": product["name"],
                "articul": product["id"],
                "categories": categories,
                "image": product["images"][0]["large"] if product["images"] else None,
                "add_image": [image["preview"] for image in product["images"][:4]],
            },
        )

        if attributes := product.get("attributes"):
            for attribute in attributes:
                if attribute["alias"] == "description":
                    product_obj.product.shortlink_data["description"] = attribute[
                        "value"
                    ]

        if weight := product.get("weight"):
            if "L" in weight["package"]:
                product_obj.product.custom_data["volume"] = weight["package"].split(
                    "L"
                )[0]
                product_obj.product.custom_data["unit"] = "л"
            elif "г" in weight["package"]:
                product_obj.product.custom_data["weight"] = weight["package"].split(
                    "г"
                )[0]
                product_obj.product.custom_data["unit"] = "г"

        product_obj.save()
        await self.save_panel_record(shop, product, link)

    async def parse_product(
        self, shop: dict, categories: List[str], product: dict, link: dict = None
    ):
        if not product["count"]:
            logger.info(f"Not in stock! {product['id']}")
            return
        _product = self.existing_products.get(str(product["id"]))
        try:
            if _product and not needs_update(_product):
                await self.save_panel_record(shop, product, link)
            else:
                await self.save_product(shop, categories, product, link)
        except ParserStopException as e:
            raise ParserStopException(e)
        except Exception:
            logger.exception(f"Cant parse product. {product['id']}")

    async def parse_products(self, shop: dict, category: dict, products: List[dict]):
        for product in products:
            categories = category["parent_names"] + [category["name"]]
            await self.parse_product(shop, categories, product)

    async def parse_category(self, shop: dict, category: dict):
        (
            offset,
            total,
            products,
        ) = (
            0,
            0,
            True,
        )
        while products or total > offset:
            if not products:
                break
            total, products = await LentaAPI.get_products(shop, category, offset)
            await self.parse_products(shop, category, products)
            offset += len(products)

    async def parse_shop(self, shop: dict, start_from: int):
        await set_location(shop)
        categories = await get_categories(shop)

        async def process_category(category, index):
            async with sem:
                logger.info(f"Parsing {category['name']}, {index}/{len(categories)}.")
                try:
                    await self.parse_category(shop, category)
                except Exception:
                    logger.exception(f"Can't parse category {category['name']} (ID: {category['id']})")
        tasks = []
        for index, category in enumerate(categories[start_from:], start=start_from):
            tasks.append(process_category(category, index))

        await asyncio.gather(*tasks)

    async def parse_link(self, shop: dict, link: dict):
        product_id = extract_product_id(link["url"])
        if not product_id:
            return
        product = await LentaAPI.get_product(shop, product_id)
        if product.get("message") == "Товар не найден":
            logger.info(f"Товар не найден. id: {product_id}")
            return
        categories = [
            category["name"] for category in product["categories"] if category["isMain"]
        ]
        await self.parse_product(shop, categories, product, link)

    async def parse_by_links(self, links: list):
        links = group_by_shop(links)

        async def process_link(shop, link, index, total):
            async with sem:
                logger.info(f"Link {index}/{total}")
                try:
                    await self.parse_link(shop, link)
                except:
                    logger.exception(f"Cant parse link: {link}")

        for key, shop_links in links.items():
            shop = find_by_shop_row_id(key)
            await set_location(shop)
            tasks = []
            for index, link in enumerate(shop_links, start=1):
                task = process_link(shop, link, index, len(shop_links))
                tasks.append(task)
            await asyncio.gather(*tasks)

    async def parse_by_categories(self, categories: list, shop: dict):
        """Parse only selected categories by id or name.

        categories: list of category ids (int/str) or names (str)
        """
        await set_location(shop)
        all_categories = await get_all_categories(shop)
        # Build lookup sets for ids and names
        ids_set = set()
        names_set = set()
        for item in categories:
            try:
                ids_set.add(int(item))
            except Exception:
                names_set.add(str(item))

        selected = [
            c for c in all_categories if c.get("id") in ids_set or c.get("name") in names_set
        ]

        if not selected:
            logger.warning("No matching categories found for provided filters")
            return

        for index, category in enumerate(selected, start=1):
            logger.info(
                f"Parsing {category['name']} ({category['id']}), {index}/{len(selected)}."
            )
            try:
                await self.parse_category(shop, category)
            except Exception:
                logger.exception(f"Can't parse category {category}")

    async def panel(self, args: dict):
        shop_id = str(args["shop_row_id"])
        shop = find_by_shop_row_id(shop_id)
        start_from = args.get("start_from", 1)
        await self.parse_shop(shop, start_from)

    async def by_links(self, args: dict):
        await self.parse_by_links(links=args["links"])

    async def by_categories(self, args: dict):
        categories = args["categories"]
        shop_id = str(args["shop_row_id"])
        shop = find_by_shop_row_id(shop_id)
        await self.parse_by_categories(categories, shop)

    async def get_shops(self, args: dict):
        await create_shops()


def parse(_task, args):

    lenta_parser = LentaParser(
        _task,
        args,
        store_id=STORE_ID,
        screenserv_app_id=SCREENSERV_APP_ID,
        screenserv_page_id=SCREENSERV_PAGE_ID,
        product_structure_id=PRODUCT_STRUCTURE_ID,
        panel_record_structure_id=PANEL_RECORD_STRUCTURE_ID,
        panel_record_validation_guard=ValidationGuard(
            condition_groups=[{"total"}, {"consec"}, {"ratio"}, {"window"}]
        ),
        product_validation_guard=ValidationGuard(
            condition_groups=[{"total", "consec", "ratio", "window"}]
        ),
    )
    lenta_parser.run()
    return {"result": True}


if DEBUG:
    parse(
        Task(),
        {
            "store": "lenta_pro",
            "shop_row_id": 	181209,
            "parsing_type": "panel",
            "disable_screenshot": False,
            # "start_from": 123,
        },
    )
    #     parse(
    #     Task(),
    #     {
    #   "store": "lenta",
    #   "parsing_type": "by_categories",
    #   "shop_row_id": 204854,
    #   "categories": ["899"],  # ids or names
    #   "disable_screenshot": False
    # },)

    # parse(
    #     Task(),
    #     AdminAPI.get_task(task_id=626541, params={"add_script_options": "true"})[
    #         "script_options"
    #     ],
    # )
