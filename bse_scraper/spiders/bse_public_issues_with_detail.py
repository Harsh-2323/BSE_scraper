import re
from datetime import datetime
import scrapy
from bse_scraper.constant import BASE_COLS, ORDERED_DETAIL_COLS


def to_iso(dmy: str) -> str:
    try:
        return datetime.strptime(dmy.strip(), "%d-%m-%Y").strftime("%Y-%m-%d")
    except Exception:
        return dmy.strip()


def split_price_band(s: str):
    s = (s or "").strip()
    if not s:
        return None, None
    parts = [p.strip() for p in re.split(r"[-–]", s)]
    if len(parts) == 1:
        return parts[0], parts[0]
    return parts[0], parts[-1]


CODE_MAP = {
    "DPI": "Debt Public Issue",
    "RI": "Rights Issue",
    "OTB": "Offer to Buy",
    "CMN": "Call Money Notice",
    "IPO": "IPO",
    "FPO": "FPO",
    "OFS": "Offer for Sale",
}


class BsePublicIssuesWithDetailSpider(scrapy.Spider):
    name = "bse_public_issues_with_detail"
    start_urls = [
        "https://www.bseindia.com/markets/PublicIssues/IPOIssues_new.aspx?id=1&Type=p"
    ]

    custom_settings = {
        "USER_AGENT": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"),
        "DOWNLOAD_DELAY": 2,
        "ROBOTSTXT_OBEY": True,
        "FEED_EXPORT_ENCODING": "utf-8",
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
    }

    def parse(self, response):
        rows = response.xpath("//table[@id='ContentPlaceHolder1_gvData']//tr[count(td) >= 8] | //table//tr[count(td) >= 8]")
        self.logger.info("Candidate rows: %d", len(rows))

        for row in rows:
            detail_href = row.xpath(".//td[1]//a/@href").get()

            def cell(n):
                return row.xpath(f"normalize-space(.//td[{n}])").get(default="")

            security_name = cell(1)
            if not security_name or security_name.lower() in {"security name", "scrip name"}:
                continue

            offer_price = cell(5)
            pmin, pmax = split_price_band(offer_price)

            base_item = {
                "security_name": security_name,
                "exchange_platform": cell(2),
                "start_date": to_iso(cell(3)),
                "end_date": to_iso(cell(4)),
                "offer_price": offer_price,
                "face_value": cell(6),
                "type_of_issue": cell(7),
                "issue_status": cell(8),
                "price_min": pmin,
                "price_max": pmax,
                "type_of_issue_long": CODE_MAP.get(cell(7).upper(), cell(7)),
            }

            if detail_href and not detail_href.lower().startswith("javascript"):
                yield response.follow(
                    detail_href,
                    callback=self.parse_detail,
                    meta={"base_item": base_item},
                )
            else:
                yield base_item

    def parse_detail(self, response):
        item = dict(response.meta["base_item"])
        item["detail_url"] = response.url

        # detail table rows
        spec_rows = response.xpath("//table//tr[count(td)>=2]")
        details = {}
        for r in spec_rows:
            label = r.xpath("normalize-space(.//td[1])").get(default="")
            value = r.xpath("normalize-space(.//td[2])").get(default="")
            if not label or (label.endswith(":") and not value):
                continue
            key = re.sub(r"\s*:\s*$", "", label).strip()
            if key and value:
                details[key] = value

        # Click Here links
        doc_links = []
        for a in response.xpath("//a[contains(translate(normalize-space(text()), 'CLICK HERE', 'click here'), 'click here')]"):
            href = a.xpath("@href").get()
            if href:
                doc_links.append({"text": "click me", "url": response.urljoin(href)})

        item["details"] = details
        item["documents"] = doc_links
        yield item
