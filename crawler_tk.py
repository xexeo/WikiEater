#!/usr/bin/env python3
"""
Crawler de Wikis para backup textual (HTML limpo) com interface Tkinter.

Objetivos principais implementados:
- Crawler educado com round-robin entre wikis/jogos;
- Controle em tempo real de threads e uso de rede (requisições/minuto);
- Consulta prévia de robots.txt com opção explícita de bypass;
- Fallback opcional para renderização de páginas com conteúdo dependente de JavaScript;
- Persistência total do estado em SQLite para pausa/retomada;
- Sem duplicar URL por wiki (normalização + UNIQUE);
- Salva somente HTML limpo em diretórios por jogo;
- Interface local para monitorar progresso, logs e controle operacional.

Observação:
- Estratégia padrão: HTTP GET + parsing local de HTML.
- Renderização headless só é usada quando o fallback JS estiver ativado.
"""

from __future__ import annotations

import html
import json
import os
import queue
import re
import signal
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from tkinter import BOTH, END, LEFT, RIGHT, VERTICAL, Button, Checkbutton, Entry, Frame, IntVar, Label, Scrollbar, Spinbox, StringVar, Text, Tk, messagebox, ttk
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urljoin, urlparse, urlunparse, urlencode
from urllib.robotparser import RobotFileParser

from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    sync_playwright = None


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
        """Armazena cada ``href`` encontrado em elementos ``<a>``."""
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
        """Ativa a coleta quando encontra um link que aponta para categoria."""
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
        """Acumula o texto visível do link de categoria atual."""
        if self._in_candidate and data.strip():
            self._text_parts.append(data.strip())

    def handle_endtag(self, tag):
        """Fecha a captura e registra a categoria extraída, se houver texto."""
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
    render_js_content: bool
    max_failures: int
    target_completion_ratio: float
    user_agent: str
    request_timeout_s: int
    js_render_timeout_s: int


class ConfigManager:
    """
    Carrega/salva configuração JSON com todos os parâmetros importantes.
    A UI altera valores em memória e pode persistir no mesmo arquivo.
    """

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.data = self._load()

    def _load(self) -> Dict:
        """Lê o arquivo JSON de configuração e retorna o conteúdo bruto."""
        with self.config_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def save(self) -> None:
        """Persiste a configuração atual no arquivo JSON do projeto."""
        with self.config_path.open("w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2)

    def reload(self) -> None:
        """Recarrega a configuração do disco, descartando mudanças não salvas."""
        self.data = self._load()

    @property
    def runtime(self) -> RuntimeSettings:
        rt = self.data["runtime"]
        return RuntimeSettings(
            max_threads=int(rt["max_threads"]),
            requests_per_minute=int(rt["requests_per_minute"]),
            bypass_robots=bool(rt["bypass_robots"]),
            render_js_content=bool(rt.get("render_js_content", False)),
            max_failures=int(rt.get("max_failures", rt.get("retry_limit", 2))),
            target_completion_ratio=float(rt["target_completion_ratio"]),
            user_agent=str(rt["user_agent"]),
            request_timeout_s=int(rt["request_timeout_s"]),
            js_render_timeout_s=int(rt.get("js_render_timeout_s", 20)),
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
        """Cria tabelas e índices usados para retomar o crawl com segurança."""
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
                    failure_count INTEGER NOT NULL DEFAULT 0,
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

            columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(urls)")}
            if "failure_count" not in columns:
                self.conn.execute("ALTER TABLE urls ADD COLUMN failure_count INTEGER NOT NULL DEFAULT 0")

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

    def has_seed_data(self) -> bool:
        """Indica se a base já possui jogos e URLs suficientes para iniciar o crawl."""
        with self.lock:
            games_count = self.conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
            urls_count = self.conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0]
            return games_count > 0 and urls_count > 0

    def get_game_rows(self) -> List[sqlite3.Row]:
        """Retorna o catálogo de jogos ordenado para consumo pela UI e pelos workers."""
        with self.lock:
            return list(self.conn.execute("SELECT * FROM games ORDER BY name"))

    def get_game_ids_with_queued_urls(self) -> List[int]:
        """Retorna os jogos que ainda possuem fila pendente para o scheduler."""
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT DISTINCT game_id
                FROM urls
                WHERE status='queued'
                ORDER BY game_id
                """
            ).fetchall()
            return [int(row["game_id"]) for row in rows]

    def claim_next_url_for_game(self, game_id: int) -> Optional[sqlite3.Row]:
        """
        Faz o pop da próxima URL da fila de um jogo.

        A ordem `depth ASC, id ASC` mantém a fila de cada jogo em breadth-first,
        enquanto o engine faz round robin entre essas filas.
        """
        with self.lock, self.conn:
            row = self.conn.execute(
                """
                SELECT * FROM urls
                WHERE game_id=? AND status='queued'
                ORDER BY depth ASC, failure_count ASC, COALESCE(last_attempt_at, first_seen_at) ASC, id ASC
                LIMIT 1
                """,
                (game_id,),
            ).fetchone()
            if not row:
                return None

            self.conn.execute(
                "UPDATE urls SET status='fetching', last_attempt_at=?, attempt_count=attempt_count+1 WHERE id=?",
                (now_iso(), row["id"]),
            )
            return self.conn.execute("SELECT * FROM urls WHERE id=?", (row["id"],)).fetchone()

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
        """Marca uma URL como concluída e registra o HTML salvo em disco."""
        with self.lock, self.conn:
            self.conn.execute(
                """
                UPDATE urls
                SET status='fetched', fetched_at=?, http_status=?, saved_html_path=?, robots_allowed=?, error=NULL, failure_count=0
                WHERE id=?
                """,
                (now_iso(), http_status, file_path, int(robots_allowed), url_id),
            )

    def mark_blocked(self, url_id: int) -> None:
        """Marca separadamente URLs bloqueadas por robots.txt."""
        with self.lock, self.conn:
            self.conn.execute(
                """
                UPDATE urls
                SET status='blocked', error='blocked by robots.txt'
                WHERE id=?
                """,
                (url_id,),
            )

    def register_failure(self, url_id: int, error: str, max_failures: int, http_status: Optional[int] = None) -> str:
        """
        Registra uma falha e decide se a URL volta para a fila ou fica em failed.

        URLs com falha temporária vão para o fim da fila do jogo; ao atingir o limite,
        ficam em `failed` até que o limite seja aumentado no runtime.
        """
        with self.lock, self.conn:
            row = self.conn.execute("SELECT failure_count FROM urls WHERE id=?", (url_id,)).fetchone()
            failure_count = int(row["failure_count"] or 0) + 1 if row else 1
            next_status = "queued" if failure_count < max(1, max_failures) else "failed"
            self.conn.execute(
                """
                UPDATE urls
                SET status=?, error=?, http_status=?, failure_count=?
                WHERE id=?
                """,
                (next_status, error[:1000], http_status, failure_count, url_id),
            )
            return next_status

    def requeue_robot_blocked_urls(self) -> int:
        """Devolve à fila URLs bloqueadas por robots quando o bypass é ativado."""
        with self.lock, self.conn:
            result = self.conn.execute(
                """
                UPDATE urls
                SET status='queued', error=NULL
                WHERE status='blocked' OR (status='failed' AND error='blocked by robots.txt')
                """
            )
            return int(result.rowcount or 0)

    def reopen_retryable_failed_urls(self, max_failures: int) -> int:
        """Reativa URLs failed cujo total de falhas ficou abaixo do novo limite."""
        with self.lock, self.conn:
            result = self.conn.execute(
                """
                UPDATE urls
                SET status='queued'
                WHERE status='failed' AND failure_count < ?
                """,
                (max(1, max_failures),),
            )
            return int(result.rowcount or 0)

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

    def get_local_link_targets(self, game_id: int, urls: List[str]) -> Dict[str, str]:
        """Resolve URLs canônicas para caminhos locais relativos dentro do backup."""
        if not urls:
            return {}

        unique_urls = sorted({canonicalize_url(url) for url in urls})
        placeholders = ",".join("?" for _ in unique_urls)
        with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT url_canonical, id
                FROM urls
                WHERE game_id=? AND url_canonical IN ({placeholders})
                """,
                [game_id, *unique_urls],
            ).fetchall()
        return {str(row["url_canonical"]): f"{int(row['id'])}.html" for row in rows}

    def insert_tags(self, url_id: int, tags: List[str]) -> None:
        """Associa tags extraídas a uma página, ignorando duplicatas."""
        with self.lock, self.conn:
            for t in tags:
                self.conn.execute("INSERT OR IGNORE INTO page_tags(url_id, tag) VALUES(?, ?)", (url_id, t[:200]))

    def stats_by_game(self) -> List[sqlite3.Row]:
        """Consolida o progresso por jogo para exibição na tabela principal."""
        with self.lock:
            return list(
                self.conn.execute(
                    """
                    SELECT g.id, g.name, g.slug, g.base_url,
                        SUM(CASE WHEN u.status='queued' THEN 1 ELSE 0 END) AS queued,
                        SUM(CASE WHEN u.status='fetching' THEN 1 ELSE 0 END) AS fetching,
                        SUM(CASE WHEN u.status='fetched' THEN 1 ELSE 0 END) AS fetched,
                        SUM(CASE WHEN u.status='blocked' THEN 1 ELSE 0 END) AS blocked,
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
        """Persiste metadados simples de execução, como o último encerramento."""
        with self.lock, self.conn:
            self.conn.execute(
                "INSERT INTO crawl_state(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                (k, v),
            )

    def reset(self) -> None:
        """Recria o banco SQLite do zero, removendo todo o estado persistido."""
        with self.lock:
            self.conn.close()

            for suffix in ("", "-wal", "-shm"):
                candidate = Path(f"{self.db_path}{suffix}")
                if candidate.exists():
                    candidate.unlink()

            self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self._init_schema()

    def close(self) -> None:
        """Fecha a conexão SQLite compartilhada pela aplicação."""
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
        """Atualiza o limite global sem recriar o objeto de rate limiting."""
        with self._lock:
            self._rpm = max(1, rpm)

    def acquire(self) -> None:
        """Bloqueia até haver uma vaga livre na janela móvel de 60 segundos."""
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
        self.started = False

        # Índice round-robin compartilhado entre workers.
        self.rr_index = 0
        self.rr_lock = threading.Lock()

        # Cache local de robots para evitar refetch.
        self.robots_cache: Dict[str, RobotFileParser] = {}
        self.robots_lock = threading.Lock()
        self.js_render_lock = threading.Lock()
        self.playwright = None
        self.browser = None

    def log(self, msg: str) -> None:
        """Envia mensagens para a fila da UI sem acessar widgets entre threads."""
        self.ui_log_queue.put(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}")

    def start(self) -> None:
        """Inicia quantidade atual de workers configurada."""
        if self.started:
            self.log("Crawler jÃ¡ estÃ¡ em execuÃ§Ã£o.")
            return

        self.stop_event.clear()
        self.pause_event.set()
        target = self.cfg.runtime.max_threads
        self._resize_workers(target)
        self.started = True
        self.log(f"Crawler iniciado com {target} threads.")

    def pause(self) -> None:
        """Pausa cooperativamente os workers já iniciados."""
        self.pause_event.clear()
        self.log("Crawler pausado.")

    def resume(self) -> None:
        """Retoma a execução após uma pausa manual."""
        self.pause_event.set()
        self.log("Crawler retomado.")

    def stop(self) -> None:
        if not self.started:
            self.log("Crawler jÃ¡ estÃ¡ parado.")
            return

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
        self._close_js_renderer()
        self.started = False
        self.log("Crawler encerrado.")

    def apply_runtime_changes(self) -> None:
        """Aplica mudanças da configuração viva (threads/rate/user-agent/bypass etc.)."""
        self.rate_limiter.update_rpm(self.cfg.runtime.requests_per_minute)
        if not self.cfg.runtime.render_js_content:
            self._close_js_renderer()
        if self.started:
            self._resize_workers(self.cfg.runtime.max_threads)
        self.log(
            f"Config runtime aplicada: threads={self.cfg.runtime.max_threads}, "
            f"rpm={self.cfg.runtime.requests_per_minute}, bypass_robots={self.cfg.runtime.bypass_robots}, "
            f"render_js_content={self.cfg.runtime.render_js_content}, max_failures={self.cfg.runtime.max_failures}"
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
        """Seleciona a próxima URL respeitando a alternância round-robin entre jogos."""
        with self.rr_lock:
            active_game_ids = self.db.get_game_ids_with_queued_urls()
            if not active_game_ids:
                self.rr_index = 0
                return None

            total_games = len(active_game_ids)
            for offset in range(total_games):
                game_id = active_game_ids[(self.rr_index + offset) % total_games]
                task = self.db.claim_next_url_for_game(game_id)
                if task:
                    self.rr_index = (self.rr_index + offset + 1) % total_games
                    return task

            self.rr_index = 0
            return None

    def _ensure_js_renderer(self):
        """Inicializa Playwright sob demanda para o fallback de conteúdo JS."""
        if sync_playwright is None:
            raise RuntimeError("Playwright não está instalado.")
        if self.browser is not None:
            return self.browser

        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        return self.browser

    def _close_js_renderer(self) -> None:
        """Fecha o navegador do fallback JS, se ele tiver sido criado."""
        with self.js_render_lock:
            if self.browser is not None:
                try:
                    self.browser.close()
                except Exception:
                    pass
                self.browser = None
            if self.playwright is not None:
                try:
                    self.playwright.stop()
                except Exception:
                    pass
                self.playwright = None

    def _robots_allowed(self, base_url: str, target_url: str) -> bool:
        """
        Consulta robots.txt por host e decide acesso.
        Mesmo quando bypass ativo, o resultado é calculado e logado.
        """
        if self.cfg.runtime.bypass_robots:
            return True

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

    def _extract_visible_text(self, html_fragment: str) -> str:
        """Reduz um fragmento HTML a texto simples para heurísticas de completude."""
        text = re.sub(r"<[^>]+>", " ", html_fragment)
        text = html.unescape(text)
        return re.sub(r"\s+", " ", text).strip()

    def _should_render_js(self, raw_html: str, clean_html: str, links: List[str]) -> bool:
        """Decide se vale tentar renderização JS quando o HTML inicial parece incompleto."""
        if not self.cfg.runtime.render_js_content:
            return False

        text_length = len(self._extract_visible_text(clean_html))
        js_markers = (
            "__NEXT_DATA__",
            'id="__next"',
            "application/ld+json",
            "window.__",
            "data-reactroot",
        )
        has_js_markers = any(marker in raw_html for marker in js_markers)
        has_main_region = bool(re.search(r"<(main|article|table|p|h1|h2)\b", clean_html, flags=re.I))
        return (text_length < 800 and len(links) < 8) or (has_js_markers and not has_main_region)

    def _render_js_content(self, url: str) -> str:
        """Renderiza a página com Playwright e retorna só a região principal quando possível."""
        selectors = ["main", "#mw-content-text", ".mw-parser-output", "article", "#content"]
        with self.js_render_lock:
            browser = self._ensure_js_renderer()
            page = browser.new_page(user_agent=self.cfg.runtime.user_agent)
            try:
                page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.cfg.runtime.js_render_timeout_s * 1000,
                )
                page.wait_for_load_state("networkidle", timeout=self.cfg.runtime.js_render_timeout_s * 1000)
                for selector in selectors:
                    locator = page.locator(selector).first
                    if locator.count():
                        try:
                            return locator.evaluate("element => element.outerHTML")
                        except Exception:
                            continue
                return page.content()
            finally:
                page.close()

    def _clean_html_and_extract(self, raw_html: str, base_url: str) -> Tuple[str, List[str], List[str]]:
        """
        Limpa HTML e extrai:
        - clean_html (somente conteúdo relevante, sem ads/imagens/scripts)
        - tags/categorias
        - links internos candidatos
        """
        # 1) Remove blocos obviamente não textuais.
        cleaned = re.sub(r"<!--.*?-->", "", raw_html, flags=re.S)
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

    def _rewrite_links_for_local_navigation(self, clean_html: str, base_url: str, game_id: int) -> str:
        """Reescreve links da wiki para navegação local relativa entre arquivos salvos."""
        href_pattern = re.compile(r'(?P<prefix>\bhref\s*=\s*)(?P<quote>["\'])(?P<href>.*?)(?P=quote)', flags=re.I)

        hrefs = [match.group("href") for match in href_pattern.finditer(clean_html)]
        canonical_candidates = [
            canonicalize_url(urljoin(base_url, href))
            for href in hrefs
            if href and not href.startswith(("#", "javascript:", "mailto:", "tel:"))
        ]
        local_targets = self.db.get_local_link_targets(game_id, canonical_candidates)

        def replace_href(match: re.Match) -> str:
            href = match.group("href")
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                return match.group(0)

            absolute_url = canonicalize_url(urljoin(base_url, href))
            local_target = local_targets.get(absolute_url)
            if local_target:
                return f'{match.group("prefix")}{match.group("quote")}{local_target}{match.group("quote")}'

            if same_host(base_url, absolute_url):
                return f'{match.group("prefix")}{match.group("quote")}#{match.group("quote")}'

            return match.group(0)

        return href_pattern.sub(replace_href, clean_html)

    def _save_html(self, game_slug: str, url_id: int, clean_html: str) -> str:
        """Salva HTML limpo em diretório por jogo, criando pastas automaticamente."""
        root = Path(self.cfg.data["storage"]["root_dir"])
        out_dir = root / game_slug
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"{url_id}.html"
        file_path.write_text(clean_html, encoding="utf-8")
        return str(file_path)

    def _worker_loop(self, local_stop_event: threading.Event) -> None:
        """Executa o ciclo fetch-processa-persiste de cada worker."""
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
                self.db.register_failure(url_id, "game not found", self.cfg.runtime.max_failures)
                continue

            base_url = game_row["base_url"]
            game_slug = game_row["slug"]

            robots_allowed = self._robots_allowed(base_url, url)
            if not robots_allowed:
                self.db.mark_blocked(url_id)
                self.log(f"BLOQUEADO por robots: {url}")
                continue

            try:
                self.rate_limiter.acquire()
                # O user-agent configurável facilita identificar o crawler e ajustar testes.
                req = Request(url, headers={"User-Agent": self.cfg.runtime.user_agent})
                with urlopen(req, timeout=self.cfg.runtime.request_timeout_s) as resp:
                    status = int(getattr(resp, "status", 200))
                    body = resp.read()
                    charset = resp.headers.get_content_charset() or "utf-8"
                text = body.decode(charset, errors="replace")

                if status >= 400:
                    self.db.register_failure(url_id, f"http {status}", self.cfg.runtime.max_failures, status)
                    self.log(f"Falha HTTP {status}: {url}")
                    continue

                clean_html, tags, links = self._clean_html_and_extract(text, base_url)
                if self._should_render_js(text, clean_html, links):
                    try:
                        rendered_html = self._render_js_content(url)
                        rendered_clean_html, rendered_tags, rendered_links = self._clean_html_and_extract(
                            rendered_html, base_url
                        )
                        if len(self._extract_visible_text(rendered_clean_html)) > len(
                            self._extract_visible_text(clean_html)
                        ):
                            clean_html = rendered_clean_html
                            tags = rendered_tags
                            links = rendered_links
                            self.log(f"Fallback JS aplicado: {url}")
                    except Exception as render_error:
                        self.log(f"Fallback JS indisponÃ­vel em {url}: {render_error}")
                next_links = [(u, depth + 1) for u in links]
                self.db.insert_discovered_links(url_id, game_id, next_links)
                self.db.insert_tags(url_id, tags)
                clean_html = self._rewrite_links_for_local_navigation(clean_html, base_url, game_id)
                saved = self._save_html(game_slug, url_id, clean_html)
                self.db.mark_fetched(url_id, status, saved, robots_allowed)

                self.log(f"OK {url} -> {saved} | links={len(next_links)} tags={len(tags)}")
            except HTTPError as e:
                self.db.register_failure(url_id, f"http {e.code}", self.cfg.runtime.max_failures, int(e.code))
                self.log(f"Erro HTTP {e.code}: {url}")
            except URLError as e:
                self.db.register_failure(url_id, f"urlerror {e.reason}", self.cfg.runtime.max_failures)
                self.log(f"Erro de rede: {url} | {e.reason}")
            except Exception as e:
                self.db.register_failure(url_id, str(e), self.cfg.runtime.max_failures)
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

        self.engine = CrawlerEngine(self.db, self.cfg_mgr, self.log_queue)

        # Variáveis de UI
        self.var_threads = IntVar(value=self.cfg_mgr.runtime.max_threads)
        self.var_rpm = IntVar(value=self.cfg_mgr.runtime.requests_per_minute)
        self.var_bypass_robots = IntVar(value=1 if self.cfg_mgr.runtime.bypass_robots else 0)
        self.var_render_js = IntVar(value=1 if self.cfg_mgr.runtime.render_js_content else 0)
        self.var_max_failures = IntVar(value=self.cfg_mgr.runtime.max_failures)
        self.var_target = StringVar(value=str(self.cfg_mgr.runtime.target_completion_ratio))

        self.tree: Optional[ttk.Treeview] = None
        self.log_text: Optional[Text] = None

        self._build_layout()
        self._bind_signals()

        # Polling periódico para atualizar tela/logs.
        self.root.after(500, self._ui_tick)

    def _build_layout(self) -> None:
        """Monta os painéis de controle, status e logs da janela principal."""
        top = Frame(self.root)
        top.pack(fill="x", padx=8, pady=8)

        Label(top, text="Threads:").pack(side=LEFT)
        Spinbox(top, from_=1, to=64, width=6, textvariable=self.var_threads).pack(side=LEFT, padx=4)

        Label(top, text="Req/min:").pack(side=LEFT)
        Spinbox(top, from_=1, to=10000, width=8, textvariable=self.var_rpm).pack(side=LEFT, padx=4)

        Checkbutton(top, text="Bypass robots.txt", variable=self.var_bypass_robots).pack(side=LEFT, padx=8)
        Checkbutton(top, text="Renderizar JS", variable=self.var_render_js).pack(side=LEFT, padx=8)
        Label(top, text="Max falhas:").pack(side=LEFT)
        Spinbox(top, from_=1, to=100, width=6, textvariable=self.var_max_failures).pack(side=LEFT, padx=4)

        Label(top, text="Meta cobertura (0-1):").pack(side=LEFT)
        Entry(top, width=6, textvariable=self.var_target).pack(side=LEFT, padx=4)

        Button(top, text="Aplicar runtime", command=self.on_apply_runtime).pack(side=LEFT, padx=6)
        Button(top, text="Salvar config", command=self.on_save_config).pack(side=LEFT, padx=6)
        Button(top, text="Criar base da config", command=self.on_seed_from_config).pack(side=LEFT, padx=6)
        Button(top, text="Iniciar", command=self.on_start).pack(side=LEFT, padx=6)
        Button(top, text="Pausar", command=self.on_pause).pack(side=LEFT, padx=6)
        Button(top, text="Retomar", command=self.on_resume).pack(side=LEFT, padx=6)
        Button(top, text="Parar", command=self.on_stop).pack(side=LEFT, padx=6)
        Button(top, text="Resetar base", command=self.on_reset).pack(side=LEFT, padx=6)

        middle = Frame(self.root)
        middle.pack(fill=BOTH, expand=True, padx=8, pady=8)

        cols = ("name", "wiki", "discovered", "queued", "fetching", "fetched", "blocked", "failed", "completion", "target90")
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
        """Conecta o fechamento da janela e sinais do sistema ao shutdown gracioso."""
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
        """Lê os campos da UI e aplica as mudanças de runtime ao engine."""
        rt = self.cfg_mgr.data["runtime"]
        rt["max_threads"] = int(self.var_threads.get())
        rt["requests_per_minute"] = int(self.var_rpm.get())
        rt["bypass_robots"] = bool(self.var_bypass_robots.get())
        rt["render_js_content"] = bool(self.var_render_js.get())
        rt["max_failures"] = int(self.var_max_failures.get())
        try:
            rt["target_completion_ratio"] = float(self.var_target.get())
        except ValueError:
            rt["target_completion_ratio"] = 0.9
            self.var_target.set("0.9")
        self.engine.apply_runtime_changes()
        if self.cfg_mgr.runtime.bypass_robots:
            requeued = self.db.requeue_robot_blocked_urls()
            if requeued:
                self.log_queue.put(f"[CONFIG] {requeued} URLs bloqueadas por robots voltaram para a fila.")
        reopened = self.db.reopen_retryable_failed_urls(self.cfg_mgr.runtime.max_failures)
        if reopened:
            self.log_queue.put(f"[CONFIG] {reopened} URLs failed voltaram para a fila pelo novo limite.")
        self._refresh_table()

    def on_save_config(self) -> None:
        """Salva em disco a configuração atualmente exibida na interface."""
        self.on_apply_runtime()
        self.cfg_mgr.save()
        self.log_queue.put("[CONFIG] arquivo de configuração salvo.")

    def on_seed_from_config(self) -> None:
        """Recarrega o JSON e popula a base com jogos e seeds definidos na configuração."""
        self.cfg_mgr.reload()
        self.var_threads.set(self.cfg_mgr.runtime.max_threads)
        self.var_rpm.set(self.cfg_mgr.runtime.requests_per_minute)
        self.var_bypass_robots.set(1 if self.cfg_mgr.runtime.bypass_robots else 0)
        self.var_render_js.set(1 if self.cfg_mgr.runtime.render_js_content else 0)
        self.var_max_failures.set(self.cfg_mgr.runtime.max_failures)
        self.var_target.set(str(self.cfg_mgr.runtime.target_completion_ratio))

        ensure_storage_dirs(self.cfg_mgr.data)
        self.db.upsert_games_and_seeds(self.cfg_mgr.data["games"])
        self.log_queue.put("[CONFIG] base populada a partir de crawler_config.json.")
        self._refresh_table()

    def on_start(self) -> None:
        """Inicia o crawler usando os valores atuais de configuração."""
        self.on_apply_runtime()
        if not self.db.has_seed_data():
            ensure_storage_dirs(self.cfg_mgr.data)
            self.db.upsert_games_and_seeds(self.cfg_mgr.data["games"])
            self.log_queue.put("[CONFIG] base criada a partir de crawler_config.json antes do início.")
            self._refresh_table()

        self.engine.start()

    def on_pause(self) -> None:
        """Pausa o crawl atual sem perder o progresso persistido."""
        self.engine.pause()

    def on_resume(self) -> None:
        """Retoma um crawl pausado anteriormente."""
        self.engine.resume()

    def on_stop(self) -> None:
        """Interrompe os workers e encerra o crawl em andamento."""
        self.engine.stop()

    def on_reset(self) -> None:
        """Apaga o estado persistido e os HTMLs salvos para recomeçar do zero."""
        confirmed = messagebox.askyesno(
            "Resetar base",
            "Isso vai apagar o banco SQLite e todos os HTMLs em 'wikis/'. Deseja continuar?",
        )
        if not confirmed:
            return

        try:
            self.engine.stop()
        except Exception:
            pass

        self._clear_storage()
        self.db.reset()
        self.db.upsert_games_and_seeds(self.cfg_mgr.data["games"])
        ensure_storage_dirs(self.cfg_mgr.data)

        if self.log_text:
            self.log_text.delete("1.0", END)
        self.log_queue = queue.Queue()
        self.engine = CrawlerEngine(self.db, self.cfg_mgr, self.log_queue)
        self.log_queue.put("[RESET] base e arquivos de saída foram recriados.")
        self._refresh_table()

    def _refresh_table(self) -> None:
        """Recalcula e redesenha a tabela de progresso por jogo."""
        if not self.tree:
            return
        # Limpa linhas atuais
        for item in self.tree.get_children():
            self.tree.delete(item)

        target = float(self.cfg_mgr.data["runtime"].get("target_completion_ratio", 0.9))
        for row in self.db.stats_by_game():
            discovered = int(row["discovered"] or 0)
            fetched = int(row["fetched"] or 0)
            blocked = int(row["blocked"] or 0)
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
                    blocked,
                    failed,
                    f"{completion*100:.1f}%",
                    target_ok,
                ),
            )

    def _drain_log_queue(self) -> None:
        """Despeja no widget de log as mensagens produzidas pelas threads."""
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
        """Executa a atualização periódica da interface sem bloquear o Tk."""
        self._refresh_table()
        self._drain_log_queue()
        self.root.after(1000, self._ui_tick)

    def _clear_storage(self) -> None:
        """Remove os diretórios de saída de cada jogo antes de um reset completo."""
        root = Path(self.cfg_mgr.data["storage"]["root_dir"])
        if root.exists():
            shutil.rmtree(root)

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
    """Inicializa a configuração, garante diretórios e sobe a aplicação Tkinter."""
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
