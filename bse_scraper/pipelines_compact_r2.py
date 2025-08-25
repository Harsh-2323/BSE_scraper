import os, re, json, hashlib
from datetime import datetime
from typing import Iterable, Optional
from urllib.parse import urlparse

from scrapy import Request
from scrapy.pipelines.files import FilesPipeline
from itemadapter import ItemAdapter

from sqlalchemy import (
    create_engine, Column, Integer, String, Date, Float, Text,
    UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

def slugify(s: Optional[str]) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^\w\s\-]+", "", s)
    s = re.sub(r"\s+", "-", s).strip("-")
    return s or "unknown"

def to_iso_date(s: Optional[str]):
    if not s: return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d %b %Y", "%d %B %Y"):
        try: return datetime.strptime(s, fmt).date()
        except Exception: pass
    try: return datetime.fromisoformat(s).date()
    except Exception: return None

def coerce_float(s: Optional[str]):
    if s is None: return None
    t = re.sub(r"[^\d.\-]", "", str(s))
    try: return float(t) if t else None
    except Exception: return None

def guess_ext_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    if path.endswith(".pdf"): return ".pdf"
    if path.endswith(".docx"): return ".docx"
    if path.endswith(".xlsx"): return ".xlsx"
    if path.endswith(".xls"): return ".xls"
    return ".bin"

class IssueCompact(Base):
    __tablename__ = "issues"
    id = Column(Integer, primary_key=True)
    # core typed columns
    security_name = Column(String(512), nullable=False)
    exchange_platform = Column(String(128))
    type_of_issue = Column(String(64))
    type_of_issue_long = Column(String(128))
    issue_status = Column(String(128))
    security_type = Column(String(64))
    start_date = Column(Date)
    end_date   = Column(Date)
    offer_price_raw = Column(String(64))
    price_min = Column(Float)
    price_max = Column(Float)
    face_value = Column(String(64))
    list_url = Column(Text)
    detail_url = Column(Text)
    # SINGLE JSONB payload for everything flexible
    # {
    #   "details": {...},
    #   "pdfs":   [{"label","url","cloud_key","cloud_url"}],
    #   "links":  [{"label","url"}],
    #   "files":  [<scrapy files results>]
    # }
    payload = Column(JSONB)
    __table_args__ = (
        UniqueConstraint("security_name", "start_date", "end_date", "detail_url", name="uq_issue_identity"),
        Index("ix_issue_dates", "start_date", "end_date"),
    )

class R2FilesPipeline(FilesPipeline):
    def get_media_requests(self, item, info) -> Iterable[Request]:
        ad = ItemAdapter(item)
        label_for_url = {}
        for k in ("pdf_links", "documents"):
            for d in ad.get(k, []) or []:
                if isinstance(d, dict) and d.get("url"):
                    label_for_url[d["url"]] = d.get("label") or ""
        file_urls = set(u for u in (ad.get("file_urls") or []) if u)
        ctx = {
            "parent_type": "issue",
            "company": ad.get("security_name") or "Unknown",
            "dt": ad.get("start_date") or ad.get("end_date") or "",
        }
        for url in sorted(file_urls):
            yield Request(url, meta={"ctx": ctx, "label": label_for_url.get(url) or ""})

    def file_path(self, request, response=None, info=None, *, item=None) -> str:
        ctx = request.meta.get("ctx") or {}
        label = (request.meta.get("label") or "").strip() or "document"
        company = ctx.get("company") or "Unknown"
        d = to_iso_date(ctx.get("dt"))
        date_folder = d.isoformat() if d else "undated"
        comp_slug = slugify(company)
        label_slug = slugify(label)
        ext = guess_ext_from_url(request.url)
        sh = hashlib.sha1(request.url.encode("utf-8")).hexdigest()[:6]
        return f"issues/{comp_slug}/{date_folder}/{label_slug}_{sh}{ext}"

class IssuesCompactDBPipeline:
    def __init__(self, database_url=None, r2_public_baseurl=None):
        self.database_url = database_url or os.getenv("DATABASE_URL")
        self.r2_public_baseurl = (r2_public_baseurl or os.getenv("R2_PUBLIC_BASEURL") or "").rstrip("/")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            database_url=crawler.settings.get("DATABASE_URL"),
            r2_public_baseurl=crawler.settings.get("R2_PUBLIC_BASEURL"),
        )

    def open_spider(self, spider):
        if not self.database_url:
            raise RuntimeError("DATABASE_URL is required")
        self.engine = create_engine(self.database_url, pool_pre_ping=True, future=True)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

    def close_spider(self, spider):
        try: self.engine.dispose()
        except Exception: pass

    def _url_to_key(self, item) -> dict:
        url_to_key = {}
        files_info = item.get("files") or []
        if isinstance(files_info, str):
            try: files_info = json.loads(files_info)
            except Exception: files_info = []
        for f in files_info:
            url, key = f.get("url"), f.get("path")
            if url and key: url_to_key[url] = key
        return url_to_key

    def process_item(self, item, spider):
        ad = ItemAdapter(item).asdict()
        sess = self.Session()
        try:
            url_to_key = self._url_to_key(ad)
            pdfs = []
            for d in ad.get("pdf_links", []) or []:
                u = d.get("url")
                key = url_to_key.get(u)
                cloud_url = f"{self.r2_public_baseurl}/{key}" if (key and self.r2_public_baseurl) else None
                pdfs.append({"label": d.get("label"), "url": u, "cloud_key": key, "cloud_url": cloud_url})

            payload = {
                "details": ad.get("details") or {},
                "pdfs": pdfs,
                "links": ad.get("links") or [],
                "files": ad.get("files") or [],
            }

            obj = IssueCompact(
                security_name=ad.get("security_name"),
                exchange_platform=ad.get("exchange_platform"),
                type_of_issue=ad.get("type_of_issue"),
                type_of_issue_long=ad.get("type_of_issue_long"),
                issue_status=ad.get("issue_status"),
                security_type=ad.get("security_type"),
                start_date=to_iso_date(ad.get("start_date")),
                end_date=to_iso_date(ad.get("end_date")),
                offer_price_raw=ad.get("offer_price"),
                price_min=coerce_float(ad.get("price_min")),
                price_max=coerce_float(ad.get("price_max")),
                face_value=ad.get("face_value"),
                list_url=ad.get("list_url"),
                detail_url=ad.get("detail_url"),
                payload=payload,
            )

            existing = (
                sess.query(IssueCompact)
                .filter(
                    IssueCompact.security_name == obj.security_name,
                    IssueCompact.start_date == obj.start_date,
                    IssueCompact.end_date == obj.end_date,
                    IssueCompact.detail_url == obj.detail_url,
                )
                .one_or_none()
            )
            if existing:
                # overwrite with latest scrape where appropriate
                for f in ("exchange_platform","type_of_issue","type_of_issue_long","issue_status",
                          "security_type","offer_price_raw","price_min","price_max",
                          "face_value","list_url"):
                    val = getattr(obj, f)
                    if val: setattr(existing, f, val)
                existing.payload = obj.payload
            else:
                sess.add(obj)
            sess.commit()
            return item
        except Exception:
            sess.rollback()
            raise
        finally:
            sess.close()
