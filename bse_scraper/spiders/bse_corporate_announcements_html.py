# bse_scraper/spiders/bse_ann_api.py

import json
from datetime import datetime, timezone
from urllib.parse import urlencode

import scrapy


class BSEAnnAPI(scrapy.Spider):
    """
    Scrapes BSE 'Latest Corporate Announcements' via the JSON API.
    Example:
      scrapy crawl bse_ann_api -a pages=10 -O outputs/announcements.json
    """

    name = "bse_ann_api"
    allowed_domains = ["api.bseindia.com", "www.bseindia.com"]

    # Make this spider independent of global Playwright/robots settings
    custom_settings = {
        # Use Scrapy's regular HTTP handler (not scrapy-playwright)
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
            "https": "scrapy.core.downloader.handlers.http.HTTPDownloadHandler",
        },
        # Ignore robots.txt for the public API endpoint
        "ROBOTSTXT_OBEY": False,
        # Be polite
        "AUTOTHROTTLE_ENABLED": True,
        "DOWNLOAD_DELAY": 0.25,
        # API often requires these headers
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.bseindia.com/corporates/ann.html",
            "Origin": "https://www.bseindia.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/119.0 Safari/537.36",
        },
    }

    BASE_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"

    # ---------- helpers ----------

    @staticmethod
    def _today_yyyymmdd():
        # Use local date; BSE dates are date-only (no tz needed)
        return datetime.now().strftime("%Y%m%d")

    @staticmethod
    def _safe_load_bse(text: str):
        """
        Normalize BSE responses into {"Table":[...], "Table1":[...]}.
        Handles cases where the API returns a JSON-encoded *string*.
        Returns an empty structure on failure.
        """
        try:
            raw = json.loads(text) if text else {}
        except Exception:
            return {"Table": [], "Table1": []}

        # Sometimes the API returns a JSON-encoded string (double-encoded)
        if isinstance(raw, str):
            try:
                raw = json.loads(raw or "{}")
            except Exception:
                return {"Table": [], "Table1": []}

        if isinstance(raw, list):
            return {"Table": raw, "Table1": []}

        if isinstance(raw, dict):
            table = raw.get("Table")
            if table is None:
                table = raw.get("data") or raw.get("Data") or []
            if isinstance(table, str):
                try:
                    table = json.loads(table)
                except Exception:
                    table = []
            return {"Table": table or [], "Table1": raw.get("Table1") or []}

        return {"Table": [], "Table1": []}

    def _build_url(self, pageno: int) -> str:
        """
        Builds the API URL with current filters.
        """
        q = {
            "pageno": pageno,
            "strCat": self.strCat,            # category (string like "AGM/EGM" or "-1")
            "strPrevDate": self.prev_date,    # YYYYMMDD
            "strScrip": self.scrip,           # scrip code/name or empty
            "strSearch": self.strSearch,      # 'P' per site
            "strToDate": self.to_date,        # YYYYMMDD
            "strType": self.segment,          # 'C' (Equity) default
            "subcategory": self.subcategory,  # subcategory string or "-1"
        }
        return f"{self.BASE_URL}?{urlencode(q)}"

    # ---------- Scrapy API ----------

    def __init__(
        self,
        pages: int = 1,
        # filters – kept close to BSE web UI names
        segment: str = "C",        # 'C' Equity | 'D' Debt/Others | 'M' MF/ETFs | 'UNLMF' Unlisted MF
        strCat: str = "-1",        # category, or -1 for all
        subcategory: str = "-1",   # subcategory, or -1 for all
        scrip: str = "",           # optional security filter
        from_date: str = None,     # dd/mm/yyyy or yyyymmdd; defaults to today
        to_date: str = None,       # dd/mm/yyyy or yyyymmdd; defaults to today
        strSearch: str = "P",      # value used by the site
        **kwargs,
    ):
        super().__init__(**kwargs)
        # pages (int)
        try:
            self.pages = max(1, int(pages))
        except Exception:
            self.pages = 1

        self.segment = segment or "C"
        self.strCat = strCat or "-1"
        self.subcategory = subcategory or "-1"
        self.scrip = scrip or ""
        self.strSearch = strSearch or "P"

        # Date handling – accept dd/mm/yyyy or yyyymmdd
        def norm_date(val: str) -> str:
            if not val:
                return self._today_yyyymmdd()
            v = val.strip()
            if len(v) == 8 and v.isdigit():
                return v
            try:
                return datetime.strptime(v, "%d/%m/%Y").strftime("%Y%m%d")
            except Exception:
                return self._today_yyyymmdd()

        self.prev_date = norm_date(from_date)
        self.to_date = norm_date(to_date)

    async def start(self):
        """
        Scrapy 2.13+ coroutine entrypoint (replaces start_requests).
        """
        for p in range(1, self.pages + 1):
            url = self._build_url(p)
            yield scrapy.Request(
                url=url,
                callback=self.parse_api,
                cb_kwargs={"pageno": p},
                dont_filter=True,
            )

    def parse_api(self, response: scrapy.http.Response, pageno: int):
        # Basic guard: some failures return an HTML error page
        ctype = response.headers.get(b"Content-Type", b"").decode().lower()
        if "json" not in ctype and not response.text.strip().startswith(("{", "[", '"')):
            self.logger.warning("Non-JSON response on page %s (Content-Type: %s)", pageno, ctype)
            return

        payload = self._safe_load_bse(response.text)
        rows = payload.get("Table") or []

        self.logger.info("Page %s: parsed %s rows", pageno, len(rows))

        for r in rows:
            if not isinstance(r, dict):
                # Defensive skip; avoids "'str' object has no attribute 'get'"
                self.logger.debug("Skipping non-dict row on page %s: %r", pageno, type(r))
                continue

            # Company / scrip
            company = (
                r.get("SLONGNAME") or r.get("COMPANYNAME") or
                r.get("SCRIP_NAME") or r.get("SC_NAME")
            )
            scrip_cd = r.get("SCRIP_CD") or r.get("SC_CODE") or r.get("SCRIPCODE")
            company_name = f"{company} - {scrip_cd}" if company and scrip_cd else (company or scrip_cd)

            # Subject/category + headline
            subject = r.get("CATEGORYNAME") or r.get("SUBCAT") or r.get("NEWSSUB")
            headline = r.get("HEADLINE") or r.get("NEWSSUB")  # for your own use if needed

            # Date/time
            dt_val = r.get("DissemDT") or r.get("NEWS_DT") or r.get("DT_TM")

            # Attachments / XBRL
            attachment_url = None
            attach = r.get("ATTACHMENTNAME")
            pdfflag = r.get("PDFFLAG")
            if attach:
                if pdfflag == 0:
                    attachment_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{attach}"
                elif pdfflag == 1:
                    attachment_url = f"https://www.bseindia.com/xml-data/corpfiling/AttachHis/{attach}"
                elif pdfflag == 2:
                    # For PDFFLAG==2, BSE provides the file via a JS-triggered route.
                    # If XML_NAME is a full URL, expose it; otherwise keep None.
                    x = r.get("XML_NAME")
                    if isinstance(x, str) and x.startswith(("http://", "https://")):
                        attachment_url = x

            # Yield in your project’s item shape
            yield {
                "company_name": company_name,
                "subject": subject,
                "datetime": dt_val,
                "attachment_url": attachment_url,
                "detail_url": attachment_url,  # keep same as before
                "exchange": "BSE",
                "page_no": pageno,
                "files": [],
                # optional extra fields (uncomment if your pipeline tolerates them):
                # "headline": headline,
                # "scrip_code": scrip_cd,
            }
