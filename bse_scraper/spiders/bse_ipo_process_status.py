# bse_scraper/spiders/bse_ipo_process_status.py
import re
from urllib.parse import unquote
import scrapy


class BseIPOProcessStatusSpider(scrapy.Spider):
    """
    Scrapes all four tables from:
      https://www.bseindia.com/markets/PublicIssues/IPOProcess_Status.aspx

    Improvements:
      - Follows numeric pages AND "Next" (Page$Next) to avoid missing later pages
      - Robust table pick: choose the first following table that actually has data rows and pager links
      - Safer header handling so we don't drop the first real data row
    """
    name = "bse_ipo_process_status"
    allowed_domains = ["bseindia.com"]
    start_urls = [
        "https://www.bseindia.com/markets/PublicIssues/IPOProcess_Status.aspx"
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "ROBOTSTXT_OBEY": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "ITEM_PIPELINES": {},  # export JSON only for this spider run
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    SECTION_TITLES = [
        "1. Draft offer document under process at BSE:",
        "2. Draft Offer Document in relation to which clarifications are sought by the Exchange and respond BSE from Issuer is awaited:",
        "3. Document in relation to which in-principal Approval issued during the fortnight:",
        "4. List of documents which have been withdrawn/ returned by the Exchange during the fortnight.",
    ]

    def parse(self, response):
        # Keep a guard against revisiting the same (section,page)
        response.meta.setdefault("seen_pages", set())

        for section_index, title in enumerate(self.SECTION_TITLES, start=1):
            # Pick the correct data table for this section
            table = self._pick_section_table(response, title)
            if not table:
                self.logger.warning("Section not found or table not recognized: %s", title)
                continue

            # Parse current page (treat as page 1 initially)
            for row_item in self._parse_table(table, section_index, title, page_index=1, response=response):
                yield row_item

            # Discover pager controls
            pager = self._extract_pager(table)
            if not pager["target"]:
                continue

            # Queue all visible numeric pages (other than 1)
            for p in pager["pages"]:
                if p > 1:
                    yield self._make_postback_request(
                        response=response,
                        event_target=pager["target"],
                        event_argument=f"Page${p}",
                        callback=self._parse_section_page,
                        cb_kwargs={
                            "section_index": section_index,
                            "section_title": title,
                            "gv_target": pager["target"],
                            "page_index": p,
                        },
                    )

            # Also, follow "Next" step-by-step to reveal further ranges
            if pager["has_next"]:
                yield self._make_postback_request(
                    response=response,
                    event_target=pager["target"],
                    event_argument="Page$Next",
                    callback=self._parse_section_next,
                    cb_kwargs={
                        "section_index": section_index,
                        "section_title": title,
                        "gv_target": pager["target"],
                        "page_index": 2,  # we’re moving to the next page
                        "seen": set([1])  # keep track of pages we’ve touched in Next-chain
                    },
                )

    # ---------- Pagination flows ----------

    def _parse_section_page(self, response, section_index, section_title, gv_target, page_index):
        # Re-find the correct table under this heading
        table = self._pick_section_table(response, section_title)
        if not table:
            return

        for row_item in self._parse_table(table, section_index, section_title, page_index, response):
            yield row_item

        # On this paginated response, queue any newly visible higher pages
        pager = self._extract_pager(table)
        for p in pager["pages"]:
            if p > page_index:  # avoid backfills/dupes
                yield self._make_postback_request(
                    response=response,
                    event_target=gv_target,
                    event_argument=f"Page${p}",
                    callback=self._parse_section_page,
                    cb_kwargs={
                        "section_index": section_index,
                        "section_title": section_title,
                        "gv_target": gv_target,
                        "page_index": p,
                    },
                )

    def _parse_section_next(self, response, section_index, section_title, gv_target, page_index, seen):
        """
        Walks forward using Page$Next to ensure we eventually expose all numeric page ranges.
        """
        table = self._pick_section_table(response, section_title)
        if not table:
            return

        # Emit rows for the current page in this "Next chain"
        for row_item in self._parse_table(table, section_index, section_title, page_index, response):
            yield row_item

        # Discover any newly visible numeric pages and schedule them (for safety)
        pager = self._extract_pager(table)
        for p in pager["pages"]:
            if p not in seen:
                seen.add(p)
                if p != page_index:  # current page already handled
                    yield self._make_postback_request(
                        response=response,
                        event_target=gv_target,
                        event_argument=f"Page${p}",
                        callback=self._parse_section_page,
                        cb_kwargs={
                            "section_index": section_index,
                            "section_title": section_title,
                            "gv_target": gv_target,
                            "page_index": p,
                        },
                    )

        # Keep clicking Next until it disappears
        if pager["has_next"]:
            next_page_index = page_index + 1
            if next_page_index not in seen:
                seen.add(next_page_index)
            yield self._make_postback_request(
                response=response,
                event_target=gv_target,
                event_argument="Page$Next",
                callback=self._parse_section_next,
                cb_kwargs={
                    "section_index": section_index,
                    "section_title": section_title,
                    "gv_target": gv_target,
                    "page_index": next_page_index,
                    "seen": seen,
                },
            )

    # ---------- Table parsing ----------

    def _parse_table(self, table_sel, section_index, section_title, page_index, response):
        """
        Parse a data table into dicts. Avoid incorrectly dropping the first row.
        """
        # Extract headers (prefer <th>, fallback to first row's <td>)
        headers = table_sel.xpath(".//tr[th]//th//text()[normalize-space()]").getall()
        headers = [self._clean_header(h) for h in headers]

        rows = table_sel.xpath(".//tr[td]")

        if not headers:
            # Try fallback headers from the first row if those cells look like headers (mostly words, not numbers)
            cand = rows[0].xpath("./td//text()[normalize-space()]").getall() if rows else []
            cand = [self._clean_header(x) for x in cand]
            # Only accept as headers if the row looks header-ish (no long numbers and unique-ish labels)
            if cand and any(re.search(r"[A-Za-z]", c) for c in cand):
                headers = cand

        # Build first row’s text to compare with headers (for safe drop)
        drop_first = False
        if headers and rows:
            first_cells = [" ".join(td.xpath(".//text()").getall()) for td in rows[0].xpath("./td")]
            first_cells = [self._clean_header(x) for x in first_cells]
            # If headers and first row texts match (case-insensitive), treat first row as header row
            if len(first_cells) == len(headers) and all((first_cells[i] or "").lower() == (headers[i] or "").lower() for i in range(len(headers))):
                drop_first = True

        # Iterate rows (skipping only if we truly saw a header row)
        start_idx = 1 if drop_first else 0
        for r in rows[start_idx:]:
            cells = [self._norm(" ".join(t.xpath(".//text()").getall())) for t in r.xpath("./td")]
            # Skip blank rows
            if not any(cells):
                continue
            item = {}
            for i, val in enumerate(cells):
                key = headers[i] if i < len(headers) and headers[i] else f"col_{i+1}"
                item[key] = val

            item["section"] = section_index
            item["section_title"] = section_title
            item["page_index"] = page_index
            item["source_url"] = response.url
            yield item

    # ---------- ASP.NET helpers ----------

    def _pick_section_table(self, response, section_title):
        """
        Choose the first following table after the section heading that:
          - has at least one data row, and
          - contains pager links (__doPostBack)
        This avoids grabbing layout tables.
        """
        title_xpath = f"//*[normalize-space(.)='{self._norm(section_title)}']"
        candidates = response.xpath(f"({title_xpath})[1]/following::table[position()<=4]")  # scan the next few tables
        for t in candidates:
            has_rows = bool(t.xpath(".//tr[td]"))
            has_pager = bool(t.xpath(".//a[contains(@href, '__doPostBack')]"))
            if has_rows:
                # Prefer the one with pager; if none has pager, take the first with rows
                if has_pager:
                    return t
                if not has_pager and not hasattr(self, "_fallback_table_chosen"):
                    # remember first rows-only as fallback if none has pager
                    setattr(self, "_fallback_table_chosen", t)
        return getattr(self, "_fallback_table_chosen", None)

    def _extract_pager(self, table_sel):
        """
        Return pager info:
          - target: __EVENTTARGET value
          - pages: set of numeric pages visible on this response
          - has_next: whether a Next link is present
        """
        hrefs = table_sel.xpath(".//a[contains(@href, '__doPostBack')]/@href").getall()
        target = self._extract_target_from_postback_list(hrefs)

        pages = set()
        has_next = False
        for href in hrefs:
            m = re.search(r"__doPostBack\((?:'|\")([^'\"]+)(?:'|\"),(?:'|\")([^'\"]+)(?:'|\")\)", href)
            if not m:
                continue
            arg = unquote(m.group(2))
            # Numerics
            pm = re.search(r"Page\$(\d+)", arg)
            if pm:
                pages.add(int(pm.group(1)))
            # Next
            if "Page$Next" in arg:
                has_next = True

        # Always include page 1 if nothing detected
        if not pages:
            pages = {1}
        return {"target": target, "pages": sorted(pages), "has_next": has_next}

    def _make_postback_request(self, response, event_target, event_argument, callback, cb_kwargs=None):
        """
        Build a FormRequest that simulates clicking a pager link (__doPostBack).
        Includes ASP.NET hidden fields.
        """
        formdata = self._collect_aspnet_hidden_fields(response)
        formdata["__EVENTTARGET"] = event_target or ""
        formdata["__EVENTARGUMENT"] = event_argument or ""
        # Some ASP.NET pages expect these anti-ajax flags absent; we keep it simple:
        return scrapy.FormRequest(
            url=response.url,
            formdata=formdata,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            callback=callback,
            cb_kwargs=cb_kwargs or {},
            dont_filter=True,
        )

    def _collect_aspnet_hidden_fields(self, response):
        def val(name):
            return response.xpath(f"//input[@type='hidden' and @name='{name}']/@value").get(default="")
        data = {
            "__VIEWSTATE": val("__VIEWSTATE"),
            "__VIEWSTATEGENERATOR": val("__VIEWSTATEGENERATOR"),
            "__EVENTVALIDATION": val("__EVENTVALIDATION"),
        }
        # include any additional hidden fields ASP.NET might require
        for inp in response.xpath("//input[@type='hidden' and @name and not(@name='__VIEWSTATE' or @name='__VIEWSTATEGENERATOR' or @name='__EVENTVALIDATION')]"):
            n = inp.xpath("@name").get()
            v = inp.xpath("@value").get(default="")
            if n and n not in data:
                data[n] = v
        return data

    @staticmethod
    def _extract_target_from_postback_list(hrefs):
        for href in hrefs or []:
            m = re.search(r"__doPostBack\((?:'|\")([^'\"]+)(?:'|\"),(?:'|\")([^'\"]+)(?:'|\")\)", href)
            if m:
                return unquote(m.group(1))
        return None

    @staticmethod
    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    @staticmethod
    def _clean_header(s: str) -> str:
        s = re.sub(r"\s+", " ", (s or "").strip())
        s = re.sub(r":\s*$", "", s)
        return s
