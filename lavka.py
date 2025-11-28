import csv
import json
import logging
import sys
import time
import traceback
import urllib
from typing import Any, List, Union
from urllib.parse import urlencode, urljoin, urlparse, urlunparse

import requests
from decouple import config
from qsator.qsator import Qsator

from base_logic.api import AdminAPI
from base_logic.base_logic import BaseParser, ValidationGuard
from base_logic.decorators import capture_api_output
from base_logic.redis import RedisClients
from base_logic.utils import ProxyManager, Task, create_proxy, needs_update

DEBUG = int(config("DEBUG", 0))
DISABLE_SCREENSHOT = False
STORE_ID = 34
SCREENSERV_APP_ID = 53
SCREENSERV_PAGE_ID = 56
BASE_ADMIN_URL = config("BASE_ADMIN_URL")
BASE_SCREENSERV_URL = config("BASE_SCREENSERV_URL", "https://mobilescreens.millionagents.com")

logger = logging.getLogger("task_logger")
task_id = None
parsing_id = None
shop_data_cache = {}


if DEBUG:
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)


PROXY_MANAGER = ProxyManager(
    redis=RedisClients.get_client(),
    type_get="random",
    proxy_types=["http_foreign"],
)


class MobileEndpoints:
    base_url = "https://grocery-authproxy-ng.lavka.yandex.net"
    headers = {
        "User-Agent": None,
        "Content-Type": "application/json",
        "x-requested-with": "XMLHttpRequest",
    }

    @classmethod
    def get_menu(cls, shop: dict) -> dict:
        url = urljoin(cls.base_url, "/4.0/eda-superapp/lavka/v1/api/v1/layout")
        headers = cls.headers.copy()
        payload = {
            "layout_slug": "grocery",
            "position": {
                "location": [
                    shop["position"]["location"][0],
                    shop["position"]["location"][1],
                ]
            },
            "additional_data": shop["additional_data"],
            "is_market_fresh": False,
            "is_supermarket": shop.get("supermarket", False)
        }
        return cls._get_data(url, "post", headers=headers, data=payload)

    @classmethod
    def get_products(cls, shop: dict, category: dict) -> dict:
        url = urljoin(cls.base_url, "/4.0/eda-superapp/lavka/v1/api/v2/modes/category")
        headers = cls.headers.copy()
        payload = {
            "modes": ["grocery"],
            "category_id": category["id"],
            "category_slug_path": {
                # "group_id": "9b27edf3d73d5ebeefccb240c3ebe622", # TODO ???
                "layout_slug": "grocery",
            },
            "position": {
                "location": [
                    shop["position"]["location"][0],
                    shop["position"]["location"][1],
                ]
            },
            "additional_data": shop["additional_data"],
            "verticals_enabled": True,
            "is_supermarket": shop.get("supermarket", False)
        }
        return cls._get_data(url, "post", headers=headers, data=payload)

    @classmethod
    def get_product_info(cls, shop: dict, product_id: Union[str, int]) -> dict:
        url = urljoin(cls.base_url, "/4.0/eda-superapp/lavka/v1/api/v1/product")
        headers = cls.headers.copy()
        payload = {
            "product_id": product_id,
            "position": {
                "location": [
                    shop["position"]["location"][0],
                    shop["position"]["location"][1],
                ]
            },
            "additional_data": shop["additional_data"],
            "enable_unavailable": True,
            "is_supermarket": shop.get("supermarket", False)
        }
        return cls._get_data(url, "post", headers=headers, data=payload)

    @staticmethod
    @capture_api_output
    def _get_data(url: str, method: str = "get", response_type="json", **kwargs) -> Any:
        attempts, max_attempts = 1, 5
        while attempts <= max_attempts:
            try:
                time.sleep(0.5)

                if headers := kwargs.get("headers"):
                    headers = headers.copy()
                if params := kwargs.get("params"):
                    url = urlunparse(urlparse(url)._replace(query=urlencode(params)))

                data = json.dumps(kwargs.get("data"))

                response = QSATOR.request(
                    method=method, url=url, headers=headers, data=data
                )
                if response_type == "json":
                    result = response["content"]
                    if isinstance(result, str):
                        result = json.loads(result)
                elif response_type == "text":
                    result = response["content"]
                return result
            except Exception as e:
                QSATOR.update_context(
                    proxy=create_proxy(PROXY_MANAGER.get_proxy(), "qsator")
                )
                logger.info(f"Can't get data, retrying {attempts}. Exception: {e}")
                time.sleep(2**attempts)
                attempts += 1
        logger.warning("Failed to get data")


QSATOR = Qsator(
    "chromium",
    MobileEndpoints.base_url,
    headless=True,
    startup_timeout=20 if not DEBUG else 20,
    connect_over_cdp=True,
    proxy=create_proxy(PROXY_MANAGER.get_proxy(), "qsator"),
)
QSATOR.use_pw_for_requests = False


MobileEndpoints.headers["User-Agent"] = QSATOR.page.evaluate(
    "() => navigator.userAgent"
)


def find_by_shop_row_id(shop_row_id) -> dict:
    global shop_data_cache
    if shop_row_id not in shop_data_cache:
        shop_data_cache[shop_row_id] = AdminAPI.find_shop_by_shop_row_id(shop_row_id)
    return shop_data_cache[shop_row_id]


def get_prices(product: dict) -> tuple:
    price = product["pricing"]["price"]

    discount_pricing = product.get("discount_pricing", dict())
    discount_price = discount_pricing.get("price")
    discount_label = discount_pricing.get("discount_label")

    if discount_pricing.get("discount_category") == "discount_for_beginners":
        discount_price, discount_label = 0, None

    price, promo_price = (price, discount_price) if discount_price else (price, 0)
    return price, promo_price, discount_label


def extract_attribute(items: list, attribute_name: str) -> str:
    return next(
        (
            item["value"].split(",")[item["attributes"].index(attribute_name)]
            for item in items
            if attribute_name in item.get("attributes", [])
        ),
        "",
    )


def get_categories(shop: dict, target_categories: List[str] = None) -> List[dict]:
    menu = MobileEndpoints.get_menu(shop)
    categories = [
        {
            **category["category_info"],
            "title": clean_text(category["category_info"]["title"]),
        }
        for category_group in menu["sections"]
        for category in category_group["categories"]
    ]
    if target_categories:
        categories = [cat for cat in categories if str(cat["id"]) in target_categories]
    return categories


def get_short_url(full_url: str) -> str:
    url = urljoin(BASE_SCREENSERV_URL, "create-shortlink/")
    payload = {"full_url": full_url}
    response = requests.post(url, data=json.dumps(payload))
    data = response.json()
    my_hash = data["hash"]
    short_link = urljoin(BASE_SCREENSERV_URL, f"shortlink/{my_hash}")
    return short_link


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


def clean_text(text: Any) -> str:
    if not isinstance(text, str):
        return text
    return (
        text.replace("\xa0", " ")
        .replace("&nbsp;", " ")
        .replace("&shy;", "")
        .replace("&nbsp", "")
    )


class YandexLavkaParser(BaseParser):
    def save_panel_record(self, shop: dict, product: dict, link):
        additional_info = dict()

        if (
            product.get("options")
            and (amount := product["options"].get("amount"))
            and (unit := product["options"].get("amountUnits"))
        ):
            if unit in ("г", "кг"):
                additional_info["weight"] = amount
                additional_info["unit"] = unit
            elif unit in ("л", "мл"):
                additional_info["volume"] = amount
                additional_info["unit"] = unit
            elif unit in ("шт", "шт."):
                additional_info["units"] = amount
                additional_info["unit"] = unit

        price, promo_price, discount_percent = get_prices(product)
        try:
            price = float(price) if price is not None else None
        except (ValueError, TypeError):
            price = None
            
        try:
            promo_price = float(promo_price) if promo_price is not None else None
        except (ValueError, TypeError):
            promo_price = None

        shortlink_data = {
            "price": price,
            "promo_price": promo_price,
            "discount_percent": discount_percent,
            # "weight": additional_info.get("weight", ""),
            "volume": additional_info.get("volume", ""),
            "unit": additional_info.get("unit", ""),
        }

        panel_record_obj = self.create_panel_record_obj(
            shop_id=shop["shop_row_id"],
            product_ext_id=product["id"],
            price=price,
            promo_price=promo_price,
            custom_data={
                "shop_row_id": shop["shop_row_id"],
                "city": shop["city"],
                "shop_id": shop["shop_id"],
                "address": shop["address"],
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
        category: dict,
        product: dict,
        product_info: dict,
        link: Union[dict, None],
    ):
        category_name = category["title"] if category else ""
        brand = extract_attribute(product_info["content"]["items"], "brandLoc")
        manufacturer = extract_attribute(
            product_info["content"]["items"], "manufacturer"
        )
        url = "https://lavka.yandex.ru/good/{0}".format(product_info["deep_link"])

        content, conditions, specs = None, None, list()
        for item in product_info["content"]["items"]:
            if item.get("title") == "Состав":
                content = item["value"]
            if item.get("title") == "Срок годности, условия хранения ":
                conditions = item["value"]
            if item.get("type") == "pfc":
                specs = [
                    {"name": trait["title"], "value": trait["value"]}
                    for trait in item["items"]
                    if trait["title"] in ["ккал", "белки", "жиры", "углеводы"]
                ]

        shortlink_data = {
            "image": (
                image_template[0].replace("{w}x{h}", "464x464")
                if (image_template := product_info.get("image_url_templates"))
                else None
            ),
            "weight": product_info["amount"],
            "title": clean_text(product_info["title"]),
            "category": clean_text(category_name),
            "specs": specs,
            "content": clean_text(content),
            "conditions": conditions,
            "brand": clean_text(brand),
            "manufacturer": clean_text(manufacturer),
        }

        product_obj = self.create_product_obj(
            ext_id=product_info["id"],
            name=clean_text(product_info["long_title"]),
            brand=brand,
            url=url,
            manufacturer=manufacturer,
            custom_data={
                "category_1": "",
                "category_2": "",
                "category_3": "",
                "category_4": category_name,
                "competitor": "lavka",
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
            logger.debug(f"Product page: {url}")

    def parse_product(
        self,
        shop: dict,
        category: dict,
        product: dict,
        product_info: dict = None,
        link: dict = None,
    ):
        try:
            if not product.get("available"):
                logger.info(f"Not in stock! {product['id']}")
                return

            _product = self.existing_products.get(str(product["id"]))
            if _product and not needs_update(_product):
                self.save_panel_record(shop, product, link)
            else:
                product_info = (
                    MobileEndpoints.get_product_info(shop, product["id"])["product"]
                    if not product_info
                    else product_info
                        )
                self.save_product(shop, category, product, product_info, link)
        except Exception:
            logger.exception(f"Cant parse product. {product['id']}")

    def parse_category(self, shop: dict, category: dict):
        products_data = MobileEndpoints.get_products(shop, category)

        for product in products_data["products"]:
            if product["type"] != "good":
                continue

            self.parse_product(shop, category, product)

    def parse_shop(self, shop: dict, target_categories: List[str] = None):
        categories = get_categories(shop, target_categories)

        for index, category in enumerate(categories, start=1):
            logger.info(
                "Parsing category: {0}. {1}/{2}".format(
                    category["title"], index, len(categories)
                )
            )
            self.parse_category(shop, category)

    def panel(self, args: dict):
        shop_id = args["shop_row_id"]
        shop = find_by_shop_row_id(shop_id)
        self.parse_shop(shop)

    def by_categories(self, args: dict):
        shop_id = args["shop_row_id"]
        shop = find_by_shop_row_id(shop_id)
        categories = [str(cat_id) for cat_id in args["categories"]]
        self.parse_shop(shop, target_categories=categories)

    def by_links(self, args: dict):
        links = args["links"]
        shop = find_by_shop_row_id(links[0]["shop_row_id"])

        for index, link in enumerate(links, start=1):
            logger.info(f"Processing link: {index}/{len(links)}")
            try:
                product_data = MobileEndpoints.get_product_info(shop, link["url"])
                product_info = product_data["product"]
                self.parse_product(shop, None, product_info, product_info, link)
            except Exception:
                logger.info(f"Product not found: {link}")

    def parse_categories(self, args: dict):
        shop = AdminAPI.find_shop_by_shop_row_id(args["shop_row_id"])
        categories = get_categories(shop)
        catalog = [{"name": cat["title"], "id": cat["id"]} for cat in categories]

        with open("categories.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["name", "id"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(catalog)


def parse(_task, args):
    global task

    task = _task

    yandex_lavka_parser = YandexLavkaParser(
        task,
        args,
        store_id=STORE_ID,
        screenserv_app_id=SCREENSERV_APP_ID,
        screenserv_page_id=SCREENSERV_PAGE_ID,
        panel_record_validation_guard=ValidationGuard(
            condition_groups=[{"total", "consec"}, {"ratio"}, {"window"}]
        ),
        product_validation_guard=ValidationGuard(
            condition_groups=[{"total", "consec"}, {"ratio"}, {"window"}]
        ),
    )
    yandex_lavka_parser.run()

    return {"result": True}


if DEBUG:
    # parse(
    #     Task(),
    #     {"shop_row_id": "276026 ", "parsing_type": "panel", "disable_screenshot": True},
    # )
    # parse(Task(), {"shop_row_id": 276027, "parsing_type": "parse_categories"})
    parse(
        Task(),
        {
            "links": [
                {'url': '6a0e482abb94467bbd0125fd5f7363c9000300010000',
                 'city': 'st. petersburg', 'store': 'lavka', 'article_id': 0, 'shop_row_id': 276027, 'article_metro': 322525}
            ],
            "parsing_type": "by_links",
            "disable_screenshot": False,
        },
    )
    # parse(
    #     Task(),
    #     {
    #         "shop_row_id": 87708,
    #         "parsing_type": "by_categories",
    #         "disable_screenshot": True,
    #         "categories": ["da51efa51cb68eb63dbc1e79074906cd"],
    #     },
    # )
    # parse(
    #     Task(),
    #     AdminAPI.get_task(task_id=590973, params={"add_script_options": "true"})[
    #         "script_options"
    #     ],
    # )
