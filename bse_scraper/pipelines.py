import os
import json
import pandas as pd
from bse_scraper.constant import BASE_COLS, ORDERED_DETAIL_COLS


def _flatten_item_for_excel(item: dict) -> dict:
    """Expand 'details' dict into top-level columns and stringify lists."""
    out = dict(item)

    details = out.pop("details", None)
    if isinstance(details, dict):
        for k, v in details.items():
            if k not in out:
                out[k] = v

    for key in ("documents", "file_urls", "files"):
        if key in out and isinstance(out[key], (list, tuple)):
            try:
                out[key] = json.dumps(out[key], ensure_ascii=False)
            except Exception:
                out[key] = str(out[key])
    return out


class ExcelAndJsonExportPipeline:
    """Writes both Excel and JSON after crawl finishes."""

    def open_spider(self, spider):
        self.rows = []
        self.raw_items = []
        base_path = spider.settings.get("EXPORT_BASE_PATH") or os.path.join("outputs", "bse_public_issues")
        os.makedirs(os.path.dirname(base_path), exist_ok=True)
        self.excel_path = base_path + ".xlsx"
        self.json_path = base_path + ".json"

    def process_item(self, item, spider):
        self.rows.append(_flatten_item_for_excel(dict(item)))
        self.raw_items.append(dict(item))
        return item

    def close_spider(self, spider):
        if not self.rows:
            return

        # ----- Excel -----
        df = pd.DataFrame(self.rows)
        cols = list(BASE_COLS)
        for c in ORDERED_DETAIL_COLS:
            if c not in cols:
                cols.append(c)
        tail = [t for t in ["documents", "file_urls", "files", "detail_url"] if t in df.columns and t not in cols]
        extras = [c for c in df.columns if c not in cols + tail]
        cols = cols + extras + tail
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df = df[cols]
        with pd.ExcelWriter(self.excel_path, engine="xlsxwriter") as xw:
            df.to_excel(xw, index=False, sheet_name="public_issues")

        # ----- JSON -----
        with open(self.json_path, "w", encoding="utf-8") as f:
            json.dump(self.raw_items, f, ensure_ascii=False, indent=2)
