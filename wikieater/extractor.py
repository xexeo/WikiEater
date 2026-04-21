from __future__ import annotations

import html
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse

BLOCKED_TAGS = {
    "script", "style", "img", "svg", "iframe", "noscript", "footer", "nav", "header", "aside", "form", "button", "video", "audio", "picture",
}
ALLOWED_TAGS = {
    "article", "main", "div", "section", "p", "ul", "ol", "li", "h1", "h2", "h3", "h4", "table", "thead", "tbody", "tr", "td", "th", "span", "strong", "em", "b", "i", "code", "pre", "a",
}
ATTR_ALLOW = {"class", "id", "data-source", "data-item-name", "href", "title", "rel"}
BLOCKED_CLASS_TOKENS = {"ad", "ads", "advert", "banner", "promo", "cookie", "sidebar", "menu", "toolbar", "nav", "footer", "header"}


class WikiHTMLProcessor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links: set[str] = set()
        self.categories: set[str] = set()
        self.output: list[str] = []
        self.block_depth = 0

    def _blocked_attrs(self, attrs: dict[str, str]) -> bool:
        class_tokens = set((attrs.get("class", "") + " " + attrs.get("id", "")).lower().replace("-", " ").split())
        return bool(class_tokens & BLOCKED_CLASS_TOKENS)

    def handle_starttag(self, tag: str, attrs_list):
        attrs = {k: v for k, v in attrs_list}

        href = attrs.get("href")
        if tag == "a" and href:
            self.links.add(href)
            if "Category:" in href or "category:" in href:
                self.categories.add(href.split("Category:", 1)[-1].replace("_", " "))

        if tag in BLOCKED_TAGS or self._blocked_attrs(attrs):
            self.block_depth += 1
            return

        if self.block_depth > 0 or tag not in ALLOWED_TAGS:
            return

        kept_attrs = []
        for key, value in attrs.items():
            if key in ATTR_ALLOW and value:
                if key == "href" and value.startswith("javascript:"):
                    continue
                kept_attrs.append((key, html.escape(value, quote=True)))

        attrs_html = "".join(f' {k}="{v}"' for k, v in kept_attrs)
        self.output.append(f"<{tag}{attrs_html}>")

    def handle_endtag(self, tag: str):
        if self.block_depth > 0:
            self.block_depth -= 1
            return
        if tag in ALLOWED_TAGS:
            self.output.append(f"</{tag}>")

    def handle_data(self, data: str):
        if self.block_depth > 0:
            return
        if data.strip():
            self.output.append(html.escape(data))


def normalize_internal_url(base_url: str, link: str) -> str | None:
    candidate = urljoin(base_url, link)
    parsed_base = urlparse(base_url)
    parsed = urlparse(candidate)

    if parsed.scheme not in {"http", "https"}:
        return None
    if parsed.netloc != parsed_base.netloc:
        return None
    if parsed.path.startswith("/wiki/Special:") or parsed.path.startswith("/wiki/File:"):
        return None

    normalized = parsed._replace(fragment="", query="")
    return urlunparse(normalized)


def clean_and_extract(html_text: str, page_url: str) -> tuple[str, set[str]]:
    parser = WikiHTMLProcessor()
    parser.feed(html_text)

    categories_html = "".join(
        f'<li class="page-category">{html.escape(cat)}</li>' for cat in sorted(c for c in parser.categories if c)
    )
    wrapped = (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        f"<meta name=\"source_url\" content=\"{html.escape(page_url, quote=True)}\">"
        "</head><body>"
        "<section class=\"wiki-page-tags\"><h2>Page tags</h2><ul>"
        f"{categories_html}</ul></section>"
        "<article class=\"wiki-item-text\">"
        f"{''.join(parser.output)}"
        "</article></body></html>"
    )
    return wrapped, parser.links
