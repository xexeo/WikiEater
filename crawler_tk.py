#!/usr/bin/env python3
"""
Crawler de Wikis para backup textual (HTML limpo) com interface Tkinter.

Objetivos principais implementados:
- Crawler educado com round-robin entre wikis/jogos;
- Controle em tempo real de threads e uso de rede (requisições/minuto);
- Consulta prévia de robots.txt com opção explícita de bypass;
- Persistência total do estado em SQLite para pausa/retomada;
- Sem duplicar URL por wiki (normalização + UNIQUE);
- Salva somente HTML limpo em diretórios por jogo;
- Interface local para monitorar progresso, logs e controle operacional.

Observação:
- Estratégia segura: NÃO usa navegador headless.
- O crawler opera apenas via HTTP GET + parsing local de HTML.
"""

from __future__ import annotations

import html
import json
import os
import queue
import re
import signal
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, Button, Checkbutton, Entry, Frame, IntVar, Label, Scrollbar, Spinbox, StringVar, Text, Tk, ttk
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse, urlunparse, urlencode
from urllib.robotparser import RobotFileParser

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# ------------------------------
# Utilidades gerais
# ------------------------------

def now_iso() -> str:
    """Retorna timestamp UTC em ISO-8601 para auditoria e persistência."""
    return datetime.now(timezone.utc).isoformat()


def slugify(name: str) -> str:
    """Gera slug estável para diretórios e chaves textuais."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", name.strip().lower())
    return s.strip("-") or "game"


def canonicalize_url(url: str) -> str:
    """
    Normaliza URL para reduzir duplicações:
    - força esquema/host em minúsculo;
    - remove fragmento (#...);
    - remove parâmetros de tracking comuns;
    - ordena querystring para idempotência.
    """
    parsed = urlparse(url)
    scheme = parsed.scheme.lower() or "https"
    netloc = parsed.netloc.lower()

    # Remove tracking e parâmetros sabidamente descartáveis
    blocked = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid"}
    query_items = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k not in blocked]
    query_items.sort(key=lambda x: (x[0], x[1]))
    query = urlencode(query_items)

    path = parsed.path or "/"
    # Remove dupla barra e barra final redundante (exceto raiz)
    path = re.sub(r"//+", "/", path)
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]

    return urlunparse((scheme, netloc, path, "", query, ""))


def same_host(base_url: str, candidate_url: str) -> bool:
    """Valida se link descoberto pertence ao mesmo host da wiki."""
    return urlparse(base_url).netloc.lower() == urlparse(candidate_url).netloc.lower()


def likely_content_url(url: str) -> bool:
    """
    Filtro conservador para evitar áreas administrativas/comentários/perfis.
    Ajuste fino pode ser feito no arquivo de configuração.
    """
    p = urlparse(url)
    path = p.path.lower()
    blocked_fragments = [
        "/special:",
        "/user:",
        "/talk:",
        "/file:",
        "/template:",
        "/category:talk",
        "/login",
        "/signin",
        "/register",
        "/edit",
        "/history",
    ]
    return not any(b in path for b in blocked_fragments)


class LinkExtractor(HTMLParser):
    """Extrator simples de links (href) via parser padrão da biblioteca."""

    def __init__(self):
        super().__init__()
        self.links: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        for k, v in attrs:
            if k.lower() == "href" and v:
                self.links.append(v)


class CategoryExtractor(HTMLParser):
    """
    Extrator conservador de tags/categorias.
    Heurística: ancora cujo href contém '/Category:' ou texto contendo 'Category:'.
    """

    def __init__(self):
        super().__init__()
        self._in_candidate = False
        self._text_parts: List[str] = []
        self.tags: set[str] = set()

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        href = ""
        for k, v in attrs:
            if k.lower() == "href":
                href = v or ""
        if "category:" in href.lower():
            self._in_candidate = True
            self._text_parts = []

    def handle_data(self, data):
        if self._in_candidate and data.strip():
            self._text_parts.append(data.strip())

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._in_candidate:
            txt = " ".join(self._text_parts).strip()
            if txt:
                self.tags.add(txt)
            self._in_candidate = False
            self._text_parts = []


# ------------------------------
# Configuração
# ------------------------------

@dataclass
class RuntimeSettings:
    """Configurações em tempo de execução ajustáveis pela interface."""
    max_threads: int
    requests_per_minute: int
    bypass_robots: bool
    target_completion_ratio: float
    user_agent: str
    request_timeout_s: int
    retry_limit: int


class ConfigManager:
    """
    Carrega/salva configuração JSON com todos os parâmetros importantes.
    A UI altera valores em memória e pode persistir no mesmo arquivo.
    """

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.data = self._load()

    def _load(self) -> Dict:
        with self.config_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def save(self) -> None:
        with self.config_path.open("w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    @property
    def runtime(self) -> RuntimeSettings:
        rt = self.data["runtime"]
        return RuntimeSettings(
            max_threads=int(rt["max_threads"]),
            requests_per_minute=int(rt["requests_per_minute"]),
            bypass_robots=bool(rt["bypass_robots"]),
            target_completion_ratio=float(rt["target_completion_ratio"]),
            user_agent=str(rt["user_agent"]),
            request_timeout_s=int(rt["request_timeout_s"]),
            retry_limit=int(rt["retry_limit"]),
        )


# ------------------------------
# Banco de dados
# ------------------------------

class DB:
    """Camada SQLite simples, thread-safe por lock global."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self.lock, self.conn:
            self.conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;

                CREATE TABLE IF NOT EXISTS games (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    slug TEXT NOT NULL UNIQUE,
                    genre TEXT NOT NULL,
                    base_url TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY,
                    game_id INTEGER NOT NULL,
                    url TEXT NOT NULL,
                    url_canonical TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    depth INTEGER NOT NULL DEFAULT 0,
                    first_seen_at TEXT NOT NULL,
                    last_attempt_at TEXT,
                    fetched_at TEXT,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    http_status INTEGER,
                    error TEXT,
                    saved_html_path TEXT,
                    robots_allowed INTEGER,
                    FOREIGN KEY(game_id) REFERENCES games(id),
                    UNIQUE(game_id, url_canonical)
                );

                CREATE TABLE IF NOT EXISTS url_links (
                    from_url_id INTEGER NOT NULL,
                    to_url_id INTEGER NOT NULL,
                    UNIQUE(from_url_id, to_url_id)
                );

                CREATE TABLE IF NOT EXISTS page_tags (
                    url_id INTEGER NOT NULL,
                    tag TEXT NOT NULL,
                    UNIQUE(url_id, tag)
                );

                CREATE TABLE IF NOT EXISTS crawl_state (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_urls_status_game ON urls(status, game_id);
                CREATE INDEX IF NOT EXISTS idx_urls_game ON urls(game_id);
                """
            )

    def upsert_games_and_seeds(self, games: List[Dict]) -> None:
        """Registra jogos e seeds iniciais sem duplicar URLs."""
        with self.lock, self.conn:
            for g in games:
                slug = slugify(g["name"])
                self.conn.execute(
                    """
                    INSERT INTO games(name, slug, genre, base_url)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(slug) DO UPDATE SET
                        name=excluded.name,
                        genre=excluded.genre,
                        base_url=excluded.base_url
                    """,
                    (g["name"], slug, g["genre"], g["wiki_url"]),
                )
                gid = self.conn.execute("SELECT id FROM games WHERE slug=?", (slug,)).fetchone()["id"]

                seeds = [g["wiki_url"]] + g.get("seed_paths", [])
                for s in seeds:
                    full = s if s.startswith("http") else urljoin(g["wiki_url"], s)
                    canon = canonicalize_url(full)
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO urls(game_id, url, url_canonical, status, depth, first_seen_at)
                        VALUES(?, ?, ?, 'queued', 0, ?)
                        """,
                        (gid, full, canon, now_iso()),
                    )

    def get_game_rows(self) -> List[sqlite3.Row]:
        with self.lock:
            return list(self.conn.execute("SELECT * FROM games ORDER BY name"))

    def get_next_url_round_robin(self, rr_index: int) -> Optional[sqlite3.Row]:
        """
        Busca próxima URL queued alternando entre jogos.
        rr_index determina ponto inicial na lista circular de jogos.
        """
        with self.lock, self.conn:
            games = self.conn.execute("SELECT id FROM games ORDER BY id").fetchall()
            if not games:
                return None
            order = [g["id"] for g in games]
            n = len(order)
            for i in range(n):
                gid = order[(rr_index + i) % n]
                row = self.conn.execute(
                    """
                    SELECT * FROM urls
                    WHERE game_id=? AND status='queued'
                    ORDER BY depth ASC, id ASC
                    LIMIT 1
                    """,
                    (gid,),
                ).fetchone()
                if row:
                    self.conn.execute(
                        "UPDATE urls SET status='fetching', last_attempt_at=?, attempt_count=attempt_count+1 WHERE id=?",
                        (now_iso(), row["id"]),
                    )
                    return self.conn.execute("SELECT * FROM urls WHERE id=?", (row["id"],)).fetchone()
            return None

    def mark_fetched(self, url_id: int, http_status: int, file_path: str, robots_allowed: bool) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                UPDATE urls
                SET status='fetched', fetched_at=?, http_status=?, saved_html_path=?, robots_allowed=?
                WHERE id=?
                """,
                (now_iso(), http_status, file_path, int(robots_allowed), url_id),
            )

    def mark_failed(self, url_id: int, error: str, http_status: Optional[int] = None) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                """
                UPDATE urls
                SET status='failed', error=?, http_status=?
                WHERE id=?
                """,
                (error[:1000], http_status, url_id),
            )

    def requeue_failed_if_retryable(self, url_id: int, retry_limit: int) -> None:
        with self.lock, self.conn:
            row = self.conn.execute("SELECT attempt_count FROM urls WHERE id=?", (url_id,)).fetchone()
            if row and row["attempt_count"] < retry_limit:
                self.conn.execute("UPDATE urls SET status='queued' WHERE id=?", (url_id,))

    def insert_discovered_links(self, from_url_id: int, game_id: int, urls: List[Tuple[str, int]]) -> None:
        """Insere URLs descobertas e relacionamentos sem duplicação."""
        with self.lock, self.conn:
            for u, depth in urls:
                canon = canonicalize_url(u)
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO urls(game_id, url, url_canonical, status, depth, first_seen_at)
                    VALUES(?, ?, ?, 'queued', ?, ?)
                    """,
                    (game_id, u, canon, depth, now_iso()),
                )
                to_row = self.conn.execute(
                    "SELECT id FROM urls WHERE game_id=? AND url_canonical=?", (game_id, canon)
                ).fetchone()
                if to_row:
                    self.conn.execute(
                        "INSERT OR IGNORE INTO url_links(from_url_id, to_url_id) VALUES(?, ?)",
                        (from_url_id, to_row["id"]),
                    )

    def insert_tags(self, url_id: int, tags: List[str]) -> None:
        with self.lock, self.conn:
            for t in tags:
                self.conn.execute("INSERT OR IGNORE INTO page_tags(url_id, tag) VALUES(?, ?)", (url_id, t[:200]))

    def stats_by_game(self) -> List[sqlite3.Row]:
        with self.lock:
            return list(
                self.conn.execute(
                    """
                    SELECT g.id, g.name, g.slug, g.base_url,
                        SUM(CASE WHEN u.status='queued' THEN 1 ELSE 0 END) AS queued,
                        SUM(CASE WHEN u.status='fetching' THEN 1 ELSE 0 END) AS fetching,
                        SUM(CASE WHEN u.status='fetched' THEN 1 ELSE 0 END) AS fetched,
                        SUM(CASE WHEN u.status='failed' THEN 1 ELSE 0 END) AS failed,
                        COUNT(u.id) AS discovered
                    FROM games g
                    LEFT JOIN urls u ON u.game_id = g.id
                    GROUP BY g.id, g.name, g.slug, g.base_url
                    ORDER BY g.name
                    """
                )
            )

    def set_state(self, k: str, v: str) -> None:
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO crawl_state(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (k, v),
            )

    def close(self) -> None:
        with self.lock:
            self.conn.close()


# ------------------------------
# Rate limiter global
# ------------------------------

class GlobalRateLimiter:
    """Controle simples de taxa global em requisições por minuto, ajustável em runtime."""

    def __init__(self, rpm: int):
        self._rpm = max(1, rpm)
        self._lock = threading.Lock()
        self._timestamps: List[float] = []

    def update_rpm(self, rpm: int) -> None:
        with self._lock:
            self._rpm = max(1, rpm)

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.time()
                window_start = now - 60.0
                self._timestamps = [t for t in self._timestamps if t >= window_start]
                if len(self._timestamps) < self._rpm:
                    self._timestamps.append(now)
                    return
                # tempo até liberar 1 slot
                wait_for = max(0.05, 60.0 - (now - min(self._timestamps)))
            time.sleep(min(wait_for, 0.5))


# ------------------------------
# Crawler principal
# ------------------------------

class CrawlerEngine:
    """Motor de crawling com workers dinâmicos e persistência total."""

    def __init__(self, db: DB, cfg: ConfigManager, ui_log_queue: queue.Queue):
        self.db = db
        self.cfg = cfg
        self.ui_log_queue = ui_log_queue
        self.stop_event = threading.Event()
        self.pause_event = threading.Event()
        self.pause_event.set()  # set => não pausado

        self.rate_limiter = GlobalRateLimiter(self.cfg.runtime.requests_per_minute)
        self.workers: List[threading.Thread] = []
        self.worker_stop_flags: List[threading.Event] = []
        self.workers_lock = threading.Lock()

        # Índice round-robin compartilhado entre workers.
        self.rr_index = 0
        self.rr_lock = threading.Lock()

        # Cache local de robots para evitar refetch.
        self.robots_cache: Dict[str, RobotFileParser] = {}
        self.robots_lock = threading.Lock()

    def log(self, msg: str) -> None:
        self.ui_log_queue.put(f"[{datetime.utcnow().strftime('%H:%M:%S')}] {msg}")

    def start(self) -> None:
        """Inicia quantidade atual de workers configurada."""
        target = self.cfg.runtime.max_threads
        self._resize_workers(target)
        self.log(f"Crawler iniciado com {target} threads.")

    def pause(self) -> None:
        self.pause_event.clear()
        self.log("Crawler pausado.")

    def resume(self) -> None:
        self.pause_event.set()
        self.log("Crawler retomado.")

    def stop(self) -> None:
        self.stop_event.set()
        self.pause_event.set()
        self.log("Encerrando crawler de forma graciosa...")
        with self.workers_lock:
            for ev in self.worker_stop_flags:
                ev.set()
            for w in self.workers:
                w.join(timeout=5)
            self.workers.clear()
            self.worker_stop_flags.clear()
        self.log("Crawler encerrado.")

    def apply_runtime_changes(self) -> None:
        """Aplica mudanças da configuração viva (threads/rate/user-agent/bypass etc.)."""
        self.rate_limiter.update_rpm(self.cfg.runtime.requests_per_minute)
        self._resize_workers(self.cfg.runtime.max_threads)
        self.log(
            f"Config runtime aplicada: threads={self.cfg.runtime.max_threads}, "
            f"rpm={self.cfg.runtime.requests_per_minute}, bypass_robots={self.cfg.runtime.bypass_robots}"
        )

    def _resize_workers(self, target: int) -> None:
        """
        Ajuste de workers em runtime:
        - sobe número de workers criando novos;
        - reduz número de workers sinalizando stop individual.
        """
        with self.workers_lock:
            alive_pairs = [(w, ev) for w, ev in zip(self.workers, self.worker_stop_flags) if w.is_alive()]
            self.workers = [p[0] for p in alive_pairs]
            self.worker_stop_flags = [p[1] for p in alive_pairs]
            current = len(self.workers)

            if target < current:
                # Encerra workers excedentes por sinal individual.
                for i in range(target, current):
                    self.worker_stop_flags[i].set()
                # Compacta listas mantendo apenas os primeiros N.
                self.workers = self.workers[:target]
                self.worker_stop_flags = self.worker_stop_flags[:target]
                return

            for idx in range(current, target):
                ev = threading.Event()
                t = threading.Thread(target=self._worker_loop, args=(ev,), name=f"worker-{idx+1}", daemon=True)
                t.start()
                self.workers.append(t)
                self.worker_stop_flags.append(ev)

    def _get_next_task(self) -> Optional[sqlite3.Row]:
        with self.rr_lock:
            task = self.db.get_next_url_round_robin(self.rr_index)
            self.rr_index += 1
            return task

    def _robots_allowed(self, base_url: str, target_url: str) -> bool:
        """
        Consulta robots.txt por host e decide acesso.
        Mesmo quando bypass ativo, o resultado é calculado e logado.
        """
        host = urlparse(base_url).netloc.lower()
        with self.robots_lock:
            if host not in self.robots_cache:
                robots_url = f"{urlparse(base_url).scheme}://{host}/robots.txt"
                rp = RobotFileParser()
                rp.set_url(robots_url)
                try:
                    rp.read()
                    self.log(f"robots.txt carregado: {robots_url}")
                except Exception as e:
                    self.log(f"Falha ao ler robots.txt ({robots_url}): {e}")
                self.robots_cache[host] = rp
            rp = self.robots_cache[host]

        try:
            allowed = rp.can_fetch(self.cfg.runtime.user_agent, target_url)
        except Exception:
            allowed = True
        return bool(allowed)

    def _clean_html_and_extract(self, html: str, base_url: str) -> Tuple[str, List[str], List[str]]:
        """
        Limpa HTML e extrai:
        - clean_html (somente conteúdo relevante, sem ads/imagens/scripts)
        - tags/categorias
        - links internos candidatos
        """
        # 1) Remove blocos obviamente não textuais.
        cleaned = re.sub(r"<!--.*?-->", "", html, flags=re.S)
        cleaned = re.sub(r"<(script|style|noscript)[^>]*>.*?</\\1>", "", cleaned, flags=re.I | re.S)
        cleaned = re.sub(r"<(img|svg|video|audio|iframe|form)[^>]*>.*?</\\1>", "", cleaned, flags=re.I | re.S)
        cleaned = re.sub(r"<(img|svg|video|audio|iframe|form)\\b[^>]*?/?>", "", cleaned, flags=re.I | re.S)

        # 2) Remove blocos com classes/ids típicos de anúncio/UI periférica.
        ad_patterns = r"(ad-|ads|advert|banner|cookie|sidebar|toolbar|recommend|related|promo)"
        cleaned = re.sub(
            rf"<([a-z0-9]+)([^>]*(class|id)=[\"'][^\"']*{ad_patterns}[^\"']*[\"'][^>]*)>.*?</\\1>",
            "",
            cleaned,
            flags=re.I | re.S,
        )

        # 3) Mantém preferencialmente região de conteúdo principal quando detectável.
        main_match = re.search(
            r"(<main\\b.*?</main>|<div[^>]+id=[\"']mw-content-text[\"'][^>]*>.*?</div>|<div[^>]+class=[\"'][^\"']*mw-parser-output[^\"']*[\"'][^>]*>.*?</div>)",
            cleaned,
            flags=re.I | re.S,
        )
        main_html = main_match.group(1) if main_match else cleaned

        # 4) Extrai categorias/tags com parser leve.
        cat_parser = CategoryExtractor()
        try:
            cat_parser.feed(main_html)
        except Exception:
            pass
        tags = sorted(cat_parser.tags)

        # 5) Descobre links internos com parser leve.
        link_parser = LinkExtractor()
        try:
            link_parser.feed(main_html)
        except Exception:
            pass

        discovered = []
        for href in link_parser.links:
            u = canonicalize_url(urljoin(base_url, href))
            if same_host(base_url, u) and likely_content_url(u):
                discovered.append(u)

        # 6) Normalização final de whitespace para reduzir ruído.
        main_html = re.sub(r"\\n{3,}", "\\n\\n", main_html)
        main_html = html.unescape(main_html)
        return main_html, tags, discovered

    def _save_html(self, game_slug: str, url_id: int, clean_html: str) -> str:
        """Salva HTML limpo em diretório por jogo, criando pastas automaticamente."""
        root = Path(self.cfg.data["storage"]["root_dir"])
        out_dir = root / game_slug
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"{url_id}.html"
        file_path.write_text(clean_html, encoding="utf-8")
        return str(file_path)

    def _worker_loop(self, local_stop_event: threading.Event) -> None:
        while not self.stop_event.is_set() and not local_stop_event.is_set():
            # Pausa cooperativa
            self.pause_event.wait(timeout=0.5)
            if not self.pause_event.is_set():
                continue

            task = self._get_next_task()
            if not task:
                time.sleep(0.5)
                continue

            game_id = int(task["game_id"])
            url_id = int(task["id"])
            url = task["url_canonical"]
            depth = int(task["depth"])

            game_row = None
            for g in self.db.get_game_rows():
                if int(g["id"]) == game_id:
                    game_row = g
                    break
            if not game_row:
                self.db.mark_failed(url_id, "game not found")
                continue

            base_url = game_row["base_url"]
            game_slug = game_row["slug"]

            robots_allowed = self._robots_allowed(base_url, url)
            if not robots_allowed and not self.cfg.runtime.bypass_robots:
                self.db.mark_failed(url_id, "blocked by robots.txt")
                self.log(f"BLOQUEADO por robots: {url}")
                continue
            if not robots_allowed and self.cfg.runtime.bypass_robots:
                self.log(f"BYPASS robots aplicado: {url}")

            try:
                self.rate_limiter.acquire()
                req = Request(url, headers={"User-Agent": self.cfg.runtime.user_agent})
                with urlopen(req, timeout=self.cfg.runtime.request_timeout_s) as resp:
                    status = int(getattr(resp, "status", 200))
                    body = resp.read()
                    charset = resp.headers.get_content_charset() or "utf-8"
                text = body.decode(charset, errors="replace")

                if status >= 400:
                    self.db.mark_failed(url_id, f"http {status}", status)
                    self.db.requeue_failed_if_retryable(url_id, self.cfg.runtime.retry_limit)
                    self.log(f"Falha HTTP {status}: {url}")
                    continue

                clean_html, tags, links = self._clean_html_and_extract(text, base_url)
                saved = self._save_html(game_slug, url_id, clean_html)

                # Salva tags e links descobertos
                next_links = [(u, depth + 1) for u in links]
                self.db.insert_discovered_links(url_id, game_id, next_links)
                self.db.insert_tags(url_id, tags)
                self.db.mark_fetched(url_id, status, saved, robots_allowed)

                self.log(f"OK {url} -> {saved} | links={len(next_links)} tags={len(tags)}")
            except HTTPError as e:
                self.db.mark_failed(url_id, f"http {e.code}", int(e.code))
                self.db.requeue_failed_if_retryable(url_id, self.cfg.runtime.retry_limit)
                self.log(f"Erro HTTP {e.code}: {url}")
            except URLError as e:
                self.db.mark_failed(url_id, f"urlerror {e.reason}")
                self.db.requeue_failed_if_retryable(url_id, self.cfg.runtime.retry_limit)
                self.log(f"Erro de rede: {url} | {e.reason}")
            except Exception as e:
                self.db.mark_failed(url_id, str(e))
                self.db.requeue_failed_if_retryable(url_id, self.cfg.runtime.retry_limit)
                self.log(f"Erro: {url} | {e}")


# ------------------------------
# Interface Tkinter
# ------------------------------

class CrawlerUI:
    """Interface local para operação do crawler e visualização de progresso."""

    def __init__(self, root: Tk, cfg_path: Path, db_path: Path):
        self.root = root
        self.root.title("Wiki Crawler — Backup textual")
        self.root.geometry("1200x760")

        self.cfg_mgr = ConfigManager(cfg_path)
        self.db = DB(db_path)
        self.log_queue: queue.Queue = queue.Queue()

        # Inicializa catálogo de jogos/seeds sempre que abre (idempotente).
        self.db.upsert_games_and_seeds(self.cfg_mgr.data["games"])

        self.engine = CrawlerEngine(self.db, self.cfg_mgr, self.log_queue)

        # Variáveis de UI
        self.var_threads = IntVar(value=self.cfg_mgr.runtime.max_threads)
        self.var_rpm = IntVar(value=self.cfg_mgr.runtime.requests_per_minute)
        self.var_bypass_robots = IntVar(value=1 if self.cfg_mgr.runtime.bypass_robots else 0)
        self.var_target = StringVar(value=str(self.cfg_mgr.runtime.target_completion_ratio))

        self.tree: Optional[ttk.Treeview] = None
        self.log_text: Optional[Text] = None

        self._build_layout()
        self._bind_signals()

        # Polling periódico para atualizar tela/logs.
        self.root.after(500, self._ui_tick)

    def _build_layout(self) -> None:
        top = Frame(self.root)
        top.pack(fill="x", padx=8, pady=8)

        Label(top, text="Threads:").pack(side=LEFT)
        Spinbox(top, from_=1, to=64, width=6, textvariable=self.var_threads).pack(side=LEFT, padx=4)

        Label(top, text="Req/min:").pack(side=LEFT)
        Spinbox(top, from_=1, to=10000, width=8, textvariable=self.var_rpm).pack(side=LEFT, padx=4)

        Checkbutton(top, text="Bypass robots.txt", variable=self.var_bypass_robots).pack(side=LEFT, padx=8)

        Label(top, text="Meta cobertura (0-1):").pack(side=LEFT)
        Entry(top, width=6, textvariable=self.var_target).pack(side=LEFT, padx=4)

        Button(top, text="Aplicar runtime", command=self.on_apply_runtime).pack(side=LEFT, padx=6)
        Button(top, text="Salvar config", command=self.on_save_config).pack(side=LEFT, padx=6)
        Button(top, text="Iniciar", command=self.on_start).pack(side=LEFT, padx=6)
        Button(top, text="Pausar", command=self.on_pause).pack(side=LEFT, padx=6)
        Button(top, text="Retomar", command=self.on_resume).pack(side=LEFT, padx=6)
        Button(top, text="Parar", command=self.on_stop).pack(side=LEFT, padx=6)

        middle = Frame(self.root)
        middle.pack(fill=BOTH, expand=True, padx=8, pady=8)

        cols = ("name", "wiki", "discovered", "queued", "fetching", "fetched", "failed", "completion", "target90")
        tree = ttk.Treeview(middle, columns=cols, show="headings", height=18)
        for c in cols:
            tree.heading(c, text=c)
            tree.column(c, width=120, anchor="w")
        tree.column("wiki", width=260)
        tree.pack(side=LEFT, fill=BOTH, expand=True)
        self.tree = tree

        sb = Scrollbar(middle, orient=VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=sb.set)
        sb.pack(side=RIGHT, fill="y")

        bottom = Frame(self.root)
        bottom.pack(fill=BOTH, expand=True, padx=8, pady=8)

        Label(bottom, text="Log operacional").pack(anchor="w")
        log_text = Text(bottom, height=14)
        log_text.pack(fill=BOTH, expand=True)
        self.log_text = log_text

    def _bind_signals(self) -> None:
        # Fechamento de janela (gracioso)
        self.root.protocol("WM_DELETE_WINDOW", self._shutdown)

        # Sinais de sistema (Linux/macOS). Em Windows alguns podem não existir.
        def handle_signal(signum, _frame):
            self.log_queue.put(f"[SIGNAL] recebido {signum}, salvando estado...")
            self._shutdown()

        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(s, handle_signal)
            except Exception:
                pass

    def on_apply_runtime(self) -> None:
        rt = self.cfg_mgr.data["runtime"]
        rt["max_threads"] = int(self.var_threads.get())
        rt["requests_per_minute"] = int(self.var_rpm.get())
        rt["bypass_robots"] = bool(self.var_bypass_robots.get())
        try:
            rt["target_completion_ratio"] = float(self.var_target.get())
        except ValueError:
            rt["target_completion_ratio"] = 0.9
            self.var_target.set("0.9")
        self.engine.apply_runtime_changes()

    def on_save_config(self) -> None:
        self.on_apply_runtime()
        self.cfg_mgr.save()
        self.log_queue.put("[CONFIG] arquivo de configuração salvo.")

    def on_start(self) -> None:
        self.engine.start()

    def on_pause(self) -> None:
        self.engine.pause()

    def on_resume(self) -> None:
        self.engine.resume()

    def on_stop(self) -> None:
        self.engine.stop()

    def _refresh_table(self) -> None:
        if not self.tree:
            return
        # Limpa linhas atuais
        for item in self.tree.get_children():
            self.tree.delete(item)

        target = float(self.cfg_mgr.data["runtime"].get("target_completion_ratio", 0.9))
        for row in self.db.stats_by_game():
            discovered = int(row["discovered"] or 0)
            fetched = int(row["fetched"] or 0)
            failed = int(row["failed"] or 0)
            queued = int(row["queued"] or 0)
            fetching = int(row["fetching"] or 0)
            completion = (fetched / discovered) if discovered else 0.0
            target_ok = "OK" if completion >= target else "--"
            self.tree.insert(
                "",
                END,
                values=(
                    row["name"],
                    row["base_url"],
                    discovered,
                    queued,
                    fetching,
                    fetched,
                    failed,
                    f"{completion*100:.1f}%",
                    target_ok,
                ),
            )

    def _drain_log_queue(self) -> None:
        if not self.log_text:
            return
        while True:
            try:
                msg = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self.log_text.insert(END, msg + "\n")
            self.log_text.see(END)

    def _ui_tick(self) -> None:
        self._refresh_table()
        self._drain_log_queue()
        self.root.after(1000, self._ui_tick)

    def _shutdown(self) -> None:
        """Finalização graciosa: para workers, salva estado e fecha DB."""
        try:
            self.engine.stop()
        except Exception:
            pass
        try:
            self.cfg_mgr.save()
        except Exception:
            pass
        try:
            self.db.set_state("last_shutdown_at", now_iso())
            self.db.close()
        except Exception:
            pass
        self.root.destroy()


# ------------------------------
# Execução principal
# ------------------------------

def ensure_storage_dirs(cfg: Dict) -> None:
    """Cria diretórios base necessários para saída do scraping."""
    root = Path(cfg["storage"]["root_dir"])
    root.mkdir(parents=True, exist_ok=True)
    for g in cfg["games"]:
        (root / slugify(g["name"])).mkdir(parents=True, exist_ok=True)


def main() -> None:
    base = Path(__file__).resolve().parent
    cfg_path = base / "crawler_config.json"
    db_path = base / "crawler_state.sqlite3"

    cfg_mgr = ConfigManager(cfg_path)
    ensure_storage_dirs(cfg_mgr.data)

    root = Tk()
    app = CrawlerUI(root, cfg_path, db_path)
    root.mainloop()


if __name__ == "__main__":
    main()
