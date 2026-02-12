from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from io import BytesIO
from typing import Any

from collections import Counter, defaultdict

try:
    from pypdf import PdfReader  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - optional local dependency for PDF parsing
    PdfReader = None  # type: ignore[assignment]


_TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall((text or "").lower())


class BM25Index:
    def __init__(self, *, k1: float = 1.5, b: float = 0.75) -> None:
        self.k1 = k1
        self.b = b
        self._docs: list[dict[str, object]] = []
        self._df: dict[str, int] = defaultdict(int)
        self._avgdl: float = 0.0

    def add(self, doc_id: str, text: str, *, payload: dict[str, object] | None = None) -> None:
        tokens = tokenize(text)
        tf = Counter(tokens)
        dl = len(tokens)
        for t in tf.keys():
            self._df[t] += 1
        self._docs.append({"id": doc_id, "tf": tf, "dl": dl, "payload": payload or {}})
        self._avgdl = sum(int(d["dl"]) for d in self._docs) / max(1, len(self._docs))

    def _idf(self, term: str) -> float:
        import math

        n = len(self._docs)
        df = self._df.get(term, 0)
        return math.log(1 + (n - df + 0.5) / (df + 0.5))

    def query(self, q: str, *, top_k: int = 10) -> list[dict[str, object]]:
        q_terms = tokenize(q)
        if not q_terms or not self._docs:
            return []

        scores: list[tuple[float, dict[str, object]]] = []
        for d in self._docs:
            tf: Counter[str] = d["tf"]  # type: ignore[assignment]
            dl = int(d["dl"])
            score = 0.0
            for term in q_terms:
                f = tf.get(term, 0)
                if not f:
                    continue
                idf = self._idf(term)
                denom = f + self.k1 * (1 - self.b + self.b * (dl / (self._avgdl or 1.0)))
                score += idf * (f * (self.k1 + 1)) / (denom or 1.0)
            if score:
                scores.append((score, d))

        scores.sort(key=lambda x: x[0], reverse=True)
        out: list[dict[str, object]] = []
        for score, d in scores[:top_k]:
            out.append({"score": score, "id": d["id"], "payload": d["payload"]})
        return out


@dataclass(frozen=True)
class DocumentSection:
    heading: str
    text: str


@dataclass(frozen=True)
class DocumentRecord:
    source_id: str
    content_type: str  # "html" | "pdf" | "unknown"
    title: str
    sections: list[DocumentSection]
    extra: dict[str, Any]


def _chunk_id(source_id: str, heading: str, text: str) -> str:
    h = hashlib.sha256()
    h.update(source_id.encode("utf-8"))
    h.update(b"\n")
    h.update((heading or "").encode("utf-8"))
    h.update(b"\n")
    h.update((text or "").encode("utf-8"))
    return h.hexdigest()[:16]


class _HeadingTextHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._heading_tag: str | None = None
        self._heading_buf: list[str] = []
        self._text_buf: list[str] = []
        self._current_heading: str = ""
        self.sections: list[DocumentSection] = []
        self.title: str = ""
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:  # noqa: ARG002
        tag = tag.lower()
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._flush_text_into_section()
            self._heading_tag = tag
            self._heading_buf = []
        elif tag == "title":
            self._in_title = True
        elif tag in {"p", "li"}:
            self._text_buf.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag == self._heading_tag:
            heading = " ".join("".join(self._heading_buf).split()).strip()
            self._current_heading = heading
            if not self.sections:
                self.sections.append(DocumentSection(heading=heading, text=""))
            elif self.sections[-1].heading != heading:
                self.sections.append(DocumentSection(heading=heading, text=""))
            self._heading_tag = None
            self._heading_buf = []
        elif tag == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        if not data or not data.strip():
            return
        if self._in_title:
            if not self.title:
                self.title = data.strip()
            return
        if self._heading_tag is not None:
            self._heading_buf.append(data.strip() + " ")
            return
        self._text_buf.append(data.strip() + " ")

    def _flush_text_into_section(self) -> None:
        text = " ".join("".join(self._text_buf).split()).strip()
        if text:
            if not self.sections:
                self.sections.append(DocumentSection(heading=self._current_heading, text=text))
            else:
                last = self.sections[-1]
                merged = (last.text + "\n" + text).strip() if last.text else text
                self.sections[-1] = DocumentSection(heading=last.heading, text=merged)
        self._text_buf = []

    def finalize(self) -> None:
        self._flush_text_into_section()


def parse_html_bytes(source_id: str, raw: bytes) -> DocumentRecord:
    parser = _HeadingTextHTMLParser()
    parser.feed(raw.decode("utf-8", errors="replace"))
    parser.finalize()
    return DocumentRecord(
        source_id=source_id,
        content_type="html",
        title=parser.title,
        sections=parser.sections,
        extra={"parser": "stdlib-htmlparser"},
    )


def _pdf_page_text(page) -> str:
    try:
        text = page.extract_text() or ""
    except Exception:
        text = ""
    text = re.sub(r"\s+\n", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def parse_pdf_bytes(source_id: str, raw: bytes) -> DocumentRecord:
    if PdfReader is None:
        raise RuntimeError("PDF parsing unavailable: install pypdf to enable PDF ingestion.")
    reader = PdfReader(BytesIO(raw))
    sections: list[DocumentSection] = []
    for i, page in enumerate(reader.pages):
        text = _pdf_page_text(page)
        if not text:
            continue
        sections.append(DocumentSection(heading=f"Page {i + 1}", text=text))
    if not sections:
        sections = [DocumentSection(heading="", text="")]
    return DocumentRecord(
        source_id=source_id,
        content_type="pdf",
        title="",
        sections=sections,
        extra={"parser": "pypdf"},
    )


def chunks_from_document(
    *,
    source_id: str,
    activity_id: int,
    jurisdiction: str,
    authority_class: str,
    publication_date: str,
    doc: DocumentRecord,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, sec in enumerate(doc.sections):
        text = (sec.text or "").strip()
        if not text:
            continue
        heading = (sec.heading or "").strip()
        chunk_id = _chunk_id(doc.source_id, heading, text)
        out.append(
            {
                "chunk_id": chunk_id,
                "activity_id": int(activity_id),
                "source_id": source_id,
                "heading": heading,
                "text": text,
                "jurisdiction": jurisdiction,
                "authority_class": authority_class,
                "publication_date": publication_date,
                "loc": f"section:{idx}",
            }
        )
    return out


def guess_content_type(*, url: str, header_content_type: str = "") -> str:
    u = (url or "").strip().lower()
    hct = (header_content_type or "").split(";")[0].strip().lower()
    if hct in {"application/pdf"}:
        return "pdf"
    if hct in {"text/html"}:
        return "html"
    if u.endswith(".pdf"):
        return "pdf"
    if u.startswith("http"):
        return "html"
    return "unknown"


def sha256_hex(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


ARP_EXTRACT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "environment_assumptions": {"type": "array", "items": {"type": "string"}},
        "participant_assumptions": {"type": "array", "items": {"type": "string"}},
        "supervision_assumptions": {"type": "array", "items": {"type": "string"}},
        "common_failure_modes": {"type": "array", "items": {"type": "string"}},
        "explicit_cautions_abort_criteria": {"type": "array", "items": {"type": "string"}},
        "explicit_limitations_from_source": {"type": "array", "items": {"type": "string"}},
    },
    "required": [
        "environment_assumptions",
        "participant_assumptions",
        "supervision_assumptions",
        "common_failure_modes",
        "explicit_cautions_abort_criteria",
        "explicit_limitations_from_source",
    ],
}


ARP_EXTRACT_SYSTEM = """You extract structured fields from a source excerpt for an Activity Risk Profile (ARP) system.

Hard rules:
- Extract ONLY statements explicitly supported by the excerpt.
- No synthesis, no interpretation, no advice, no scoring, no compliance claims.
- If the excerpt does not explicitly state something for a field, return an empty list for that field.
- Keep each bullet short and specific (1 sentence).
- Output must be valid JSON with no extra keys.
"""


def arp_extract_user_prompt(*, activity: str, heading: str, excerpt: str) -> str:
    return f"""Activity: {activity}
Section heading: {heading or "(none)"}

Excerpt:
{excerpt}
"""


MANDATORY_TITLES = [
    "Activity overview",
    "Why this activity creates risk",
    "What is commonly underestimated",
    "Good practice signals (aggregated)",
    "Where context changes everything",
    "Common failure modes",
    "What this does not tell you",
    "Source context",
    "Review metadata",
]


ARP_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "Activity overview": {"type": "string"},
        "Why this activity creates risk": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "paragraph": {"type": "string"},
                "bullets": {"type": "array", "minItems": 3, "maxItems": 4, "items": {"type": "string"}},
            },
            "required": ["paragraph", "bullets"],
        },
        "What is commonly underestimated": {"type": "array", "minItems": 4, "maxItems": 6, "items": {"type": "string"}},
        "Good practice signals (aggregated)": {"type": "array", "minItems": 4, "maxItems": 6, "items": {"type": "string"}},
        "Where context changes everything": {"type": "array", "minItems": 4, "maxItems": 6, "items": {"type": "string"}},
        "Common failure modes": {"type": "array", "minItems": 4, "maxItems": 6, "items": {"type": "string"}},
        "What this does not tell you": {"type": "array", "minItems": 4, "maxItems": 6, "items": {"type": "string"}},
        "Source context": {"type": "string"},
        "Review metadata": {"type": "string"},
    },
    "required": MANDATORY_TITLES,
}


ARP_WRITE_SYSTEM = """SYSTEM PROMPT — Generate Activity Risk Profile (ARP)
Role

You are generating a leader-facing Activity Risk Profile (ARP) for educational travel and school-sponsored activities.

Hard constraints:
- No provider scoring, no safe/unsafe conclusions, no compliance claims.
- Use observational, conditional language.
- Keep it one-page and decision-support oriented.

Mandatory Output Structure

Use the following section titles exactly and only once, in this order:

Activity overview
Why this activity creates risk
What is commonly underestimated
Good practice signals (aggregated)
Where context changes everything
Common failure modes
What this does not tell you
Source context
Review metadata

Output must be valid JSON with exactly those keys (and required shapes)."""


def validate_arp_json(obj: Any) -> tuple[bool, str]:
    if not isinstance(obj, dict):
        return False, "ARP must be a JSON object"
    missing = [k for k in MANDATORY_TITLES if k not in obj]
    if missing:
        return False, f"Missing keys: {missing}"
    if not isinstance(obj.get("Activity overview"), str):
        return False, "Activity overview must be a string"
    why = obj.get("Why this activity creates risk")
    if not isinstance(why, dict) or not isinstance(why.get("paragraph"), str) or not isinstance(why.get("bullets"), list):
        return False, "Why this activity creates risk must be {paragraph, bullets}"
    return True, ""


def render_arp_json_to_markdown(activity_name: str, arp: dict[str, Any]) -> str:
    ok, err = validate_arp_json(arp)
    if not ok:
        raise ValueError(err)
    lines: list[str] = []
    lines.append("# Activity Risk Profile")
    lines.append("")
    lines.append(f"**Activity:** {activity_name}")
    lines.append("")

    lines.append("## Activity overview")
    lines.append(str(arp["Activity overview"]).strip())
    lines.append("")

    why = arp["Why this activity creates risk"]
    lines.append("## Why this activity creates risk")
    lines.append(str(why.get("paragraph") or "").strip())
    lines.append("")
    for b in (why.get("bullets") or []):
        lines.append(f"- {str(b).strip()}")
    lines.append("")

    def bullets(title: str) -> None:
        lines.append(f"## {title}")
        for b in (arp.get(title) or []):
            lines.append(f"- {str(b).strip()}")
        lines.append("")

    bullets("What is commonly underestimated")
    bullets("Good practice signals (aggregated)")
    bullets("Where context changes everything")
    bullets("Common failure modes")
    bullets("What this does not tell you")

    lines.append("## Source context")
    lines.append(str(arp["Source context"]).strip())
    lines.append("")
    lines.append("## Review metadata")
    lines.append(str(arp["Review metadata"]).strip())
    lines.append("")
    return "\n".join(lines).strip() + "\n"
