#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rutube Full Scraper / Analyzer + Autonomous RUTUBE TV Discovery

РЕЖИМЫ:

1. Автономный режим:
       python m3u_rutube.py

   Ничего вводить не требуется.

   Скрипт:
       - открывает RUTUBE TV Online;
       - обнаруживает категории;
       - обходит страницы каталога;
       - обнаруживает карточки телеканалов;
       - получает video ID;
       - получает video ID карточек;
       - после Discovery обращается к play/options;
       - получает ВСЕ подписанные HLS-потоки через bl.rutube.ru/livestream;
       - сохраняет ВСЕ найденные записи;
       - НЕ выполняет дедупликацию;
       - генерирует M3U для IPTV/Televizo;
       - генерирует JSON;
       - генерирует JSONL;
       - генерирует TXT;
       - пишет лог;
       - продолжает работу при ошибке отдельного канала.

2. Старый режим одного видео:

       python m3u_rutube.py VIDEO_ID

   Сохраняется исходная логика:
       - авторизация Rutube LiST;
       - visitor;
       - award;
       - video balancer;
       - master M3U8;
       - разбор HLS;
       - тестирование streams;
       - JSON/TXT/M3U/JSONL.

ВАЖНЫЕ ПРАВИЛА TV DISCOVERY:

    - НИКАКОЙ ДЕДУПЛИКАЦИИ.
    - Не удалять одинаковые названия.
    - Не удалять одинаковые video_id.
    - Не удалять одинаковые URL.
    - Не объединять записи разных категорий.
    - Если один канал встречается в нескольких категориях/полках,
      каждая запись сохраняется отдельно.
    - Не фильтровать "Союз", "Союзный", "БелРос" и любые
      другие названия.
    - Не ограничиваться заранее заданным списком каналов.
    - HLS URL сохраняются полностью, включая s= и e=.
    - URL получают заново при каждом запуске.

ПОСЛЕДОВАТЕЛЬНАЯ ЦЕПОЧКА (не менять порядок):

    1. DISCOVERY
       - обнаруживает полки (shelves / resources / categories)
       - источники: API feeds/live + HTML fallback

    2. ПОЛКИ
       - из каждой обнаруженной полки берём только реальные
         карточки TV / live-каналов

    3. КАРТОЧКИ
       - из каждой карточки извлекаем video_id
       - НЕ берём технические ID:
         tvfavorites, history, topic, assets, banner-*,
         autowidget, feedsource-технические и т.п.

    4. VIDEO_ID → RUTUBE BALANCER
       - get_live_options(video_id)
       - извлекаются ВСЕ HLS URL из live_streams.hls
       - источник потоков: https://bl.rutube.ru/livestream/
       - если структура ответа API изменится, выполняется рекурсивный
         поиск всех bl.rutube.ru/livestream/*.m3u8 в ответе

    5. ВСЕ найденные HLS-ссылки
       - записываются в M3U / JSON / JSONL / TXT
       - категория/полка каждой карточки сохраняется

Discovery — источник того, ЧТО собирать.
Сбор ссылок — второй этап на основе результата Discovery.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import logging
import os
import re
import sys
import time
import zipfile

from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import (
    parse_qs,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)

import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except ImportError:
    Retry = None


# ============================================================
# VERSION
# ============================================================

VERSION = "2.3.1-TV-ARTIFACT-400-BALANCER"


# ============================================================
# AUTONOMOUS TV CONFIGURATION
# ============================================================

DISCOVERY_SOURCE_URL = "https://rutube.ru/feeds/live/"

TV_PROGRAM_SOURCE_URL = (
    "https://rutube.ru/feeds/live/tvprogramm/"
)

FALLBACK_CATEGORY_URLS = {
    "Федеральные":
        "https://rutube.ru/feeds/live/tvprogramm/federal/",

    "Региональные":
        "https://rutube.ru/feeds/live/tvprogramm/regional/",

    "Новости":
        "https://rutube.ru/feeds/live/tvprogramm/news/",

    "Развлекательные":
        "https://rutube.ru/feeds/live/tvprogramm/entertainment/",

    "Кино и сериалы":
        "https://rutube.ru/feeds/live/tvprogramm/movie/",

    "Спорт":
        "https://rutube.ru/feeds/live/tvprogramm/sport/",

    "Детские":
        "https://rutube.ru/feeds/live/tvprogramm/kids/",

    "Мультфильмы":
        "https://rutube.ru/feeds/live/tvprogramm/cartoons/",

    "Музыка":
        "https://rutube.ru/feeds/live/tvprogramm/music/",

    "Познавательные":
        "https://rutube.ru/feeds/live/tvprogramm/educational/",

    "Телемагазин":
        "https://rutube.ru/feeds/live/tvprogramm/teleshop/",

    "18+":
        "https://rutube.ru/feeds/live/tvprogramm/18/",

    "Религия":
        "https://rutube.ru/feeds/live/tvprogramm/religion/",
}

DEFAULT_MAX_PAGES = 100
DEFAULT_MAX_EMPTY_PAGES = 2

RUTUBE_GROUP = "Рутуб кабельная"

M3U_EPG_URL = "https://iptvx.one/EPG"

M3U_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

OUTPUT_DIR = "rutube_output"

M3U_FILENAME = "rutube_tv.m3u"
JSON_FILENAME = "rutube_tv.json"
JSONL_FILENAME = "rutube_tv.jsonl"
TXT_FILENAME = "rutube_tv.txt"

DISCOVERY_HTML_FILENAME = "rutube_tv_discovery_debug.html"


# ============================================================
# CUMULATIVE TV DISCOVERY
# ============================================================

TV_TARGET_COUNT = 400

TV_ARTIFACT_MARKER_RE = re.compile(
    r"\*\s*телеканалы\s*\*",
    re.IGNORECASE,
)

YOUTUBE_DISCOVERY_MAX_CANDIDATES = 600

YOUTUBE_DISCOVERY_QUERIES = (
    "телеканалы России прямой эфир",
    "российские телеканалы live",
    "телеканалы Москва прямой эфир",
    "региональные телеканалы России live",
    "новости телеканал прямой эфир Россия",
    "спортивные телеканалы России live",
    "детские телеканалы России live",
    "музыкальные телеканалы России live",
)


# ============================================================
# EXCEPTION
# ============================================================

class RutubeScrapperError(Exception):
    """Base exception for Rutube scraper."""


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class StreamInfo:
    resolution: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None

    bandwidth: Optional[int] = None
    bandwidth_kbps: Optional[float] = None

    average_bandwidth: Optional[int] = None

    codecs: Optional[str] = None
    mime_type: Optional[str] = None

    frame_rate: Optional[float] = None

    audio: Optional[str] = None
    video: Optional[str] = None

    url: str = ""

    protocol: str = "HLS"

    url_hash: str = ""

    alive: Optional[bool] = None
    http_status: Optional[int] = None
    content_type: Optional[str] = None
    response_size: Optional[int] = None
    response_time_ms: Optional[float] = None

    quality_score: float = 0.0

    source_index: int = 0

    raw_attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class VideoInfo:
    requested_id: str = ""
    internal_id: str = ""

    title: Optional[str] = None
    description: Optional[str] = None

    duration: Optional[float] = None

    author: Optional[str] = None
    author_id: Optional[str] = None

    category: Optional[str] = None

    created_at: Optional[str] = None
    published_at: Optional[str] = None

    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RequestStat:
    name: str
    method: str
    url: str

    status: Optional[int] = None

    ok: bool = False

    duration_ms: Optional[float] = None

    response_size: Optional[int] = None

    error: Optional[str] = None


# ============================================================
# TV ARTIFACT / DISCOVERY HELPERS
# ============================================================

def _contains_tv_artifact_marker(value: Any) -> bool:
    """True только для контекста, содержащего *Телеканалы*."""
    if isinstance(value, str):
        return bool(TV_ARTIFACT_MARKER_RE.search(value))

    if isinstance(value, dict):
        return any(
            _contains_tv_artifact_marker(k)
            or _contains_tv_artifact_marker(v)
            for k, v in value.items()
        )

    if isinstance(value, (list, tuple)):
        return any(
            _contains_tv_artifact_marker(v)
            for v in value
        )

    return False


def _walk_artifact_objects(
    value: Any,
    marked: bool = False,
):
    """
    Yield nested dicts and carry the *Телеканалы* marker down
    to children.
    """
    if isinstance(value, dict):
        local_marked = marked or _contains_tv_artifact_marker(value)

        yield value, local_marked

        for child in value.values():
            yield from _walk_artifact_objects(
                child,
                local_marked,
            )

        return

    if isinstance(value, (list, tuple)):
        local_marked = marked or _contains_tv_artifact_marker(value)

        for child in value:
            yield from _walk_artifact_objects(
                child,
                local_marked,
            )


def _first_string(
    value: Any,
    keys: Tuple[str, ...],
) -> Optional[str]:
    if not isinstance(value, dict):
        return None

    for key in keys:
        candidate = value.get(key)

        if isinstance(candidate, str):
            text = candidate.strip()

            if text:
                return text

    return None


def _extract_video_id_from_text(
    value: Any,
) -> Optional[str]:
    if not isinstance(value, str):
        return None

    text = value.strip()

    patterns = (
        r"/live/video/([0-9a-fA-F]{8,64})",
        r"/video/([0-9a-fA-F]{8,64})",
        r"[?&]video_id=([0-9a-fA-F]{8,64})",
        r'"video_id"\s*:\s*"([^"]+)"',
        r'"videoId"\s*:\s*"([^"]+)"',
        r'"id"\s*:\s*"([0-9a-fA-F]{8,64})"',
    )

    for pattern in patterns:
        match = re.search(
            pattern,
            text,
            re.IGNORECASE,
        )

        if match:
            candidate = match.group(1).strip()

            if candidate:
                return candidate

    return None


def _normalize_tv_identity(
    name: Any,
) -> str:
    text = str(name or "").strip().lower()

    text = re.sub(
        r"\s+",
        " ",
        text,
    )

    text = re.sub(
        r"^\s*\d+\s*[\.\):-]\s*",
        "",
        text,
    )

    text = re.sub(
        r"\s*\(\s*[+-]\d+\s*\)\s*$",
        "",
        text,
    )

    return text.strip()


def _artifact_roots(
    root: Path,
):
    if not root.exists():
        return []

    if root.is_file():
        return [root]

    result = []

    for path in root.rglob("*"):
        if path.is_file():
            result.append(path)

    return result


def _download_previous_workflow_artifacts(
    output_dir: Path,
) -> List[Path]:
    """
    Download artifacts from previous completed GitHub Actions runs.

    If GitHub API is unavailable, the function falls back to directories
    already mounted in the workspace.
    """
    downloaded: List[Path] = []

    workspace = Path(
        os.getenv(
            "GITHUB_WORKSPACE",
            ".",
        )
    )

    fallback_dirs = [
        workspace / "rutube_output" / "_previous_workflow_artifacts",
        workspace / "_previous_workflow_artifacts",
        output_dir / "_previous_workflow_artifacts",
    ]

    for directory in fallback_dirs:
        if directory.exists():
            downloaded.append(directory)

    repository = os.getenv("GITHUB_REPOSITORY")
    token = os.getenv("GITHUB_TOKEN")
    current_run_id = os.getenv("GITHUB_RUN_ID")

    if not repository or not token:
        return downloaded

    previous_dir = (
        output_dir /
        "_previous_workflow_artifacts"
    )

    previous_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    session = requests.Session()

    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": M3U_USER_AGENT,
    })

    try:
        runs_url = (
            "https://api.github.com/repos/"
            f"{repository}/actions/runs"
        )

        params = {
            "status": "completed",
            "per_page": 20,
        }

        response = session.get(
            runs_url,
            params=params,
            timeout=20,
        )

        response.raise_for_status()

        runs_data = response.json()

        runs = runs_data.get(
            "workflow_runs",
            [],
        )

        artifact_count = 0

        for run in runs:
            run_id = str(
                run.get("id") or ""
            )

            if (
                not run_id
                or run_id == str(current_run_id or "")
            ):
                continue

            artifacts_url = (
                "https://api.github.com/repos/"
                f"{repository}/actions/runs/"
                f"{run_id}/artifacts"
            )

            artifacts_response = session.get(
                artifacts_url,
                params={"per_page": 100},
                timeout=20,
            )

            if not artifacts_response.ok:
                continue

            artifacts_data = (
                artifacts_response.json()
            )

            artifacts = artifacts_data.get(
                "artifacts",
                [],
            )

            for artifact in artifacts:
                if artifact.get("expired"):
                    continue

                name = str(
                    artifact.get("name") or ""
                )

                name_lower = name.lower()

                if not any(
                    token_name in name_lower
                    for token_name in (
                        "rutube",
                        "tv",
                        "discovery",
                        "channel",
                        "artifact",
                    )
                ):
                    continue

                artifact_id = artifact.get("id")

                if not artifact_id:
                    continue

                zip_url = (
                    "https://api.github.com/repos/"
                    f"{repository}/actions/artifacts/"
                    f"{artifact_id}/zip"
                )

                try:
                    zip_response = session.get(
                        zip_url,
                        timeout=60,
                    )

                    if not zip_response.ok:
                        continue

                    archive_path = (
                        previous_dir /
                        f"{artifact_id}.zip"
                    )

                    archive_path.write_bytes(
                        zip_response.content
                    )

                    extract_dir = (
                        previous_dir /
                        str(artifact_id)
                    )

                    extract_dir.mkdir(
                        parents=True,
                        exist_ok=True,
                    )

                    with zipfile.ZipFile(
                        archive_path,
                        "r",
                    ) as archive:
                        archive.extractall(
                            extract_dir
                        )

                    downloaded.append(
                        extract_dir
                    )

                    artifact_count += 1

                except Exception as exc:
                    logging.warning(
                        "ARTIFACT DOWNLOAD FAILED: %s",
                        exc,
                    )

            if artifact_count >= 20:
                break

    except Exception as exc:
        logging.warning(
            "GITHUB ARTIFACT API FAILED: %s",
            exc,
        )

    return downloaded


def load_previous_tv_cards(
    output_dir: Path,
) -> List[Dict[str, Any]]:
    """
    Load TV cards from previous Workflow artifacts.

    Only objects belonging to a context marked with
    *Телеканалы* are considered authoritative.
    """
    roots = _download_previous_workflow_artifacts(
        output_dir
    )

    cards: List[Dict[str, Any]] = []

    seen_files = set()

    for root in roots:
        for path in _artifact_roots(root):
            try:
                resolved = path.resolve()
            except Exception:
                resolved = path

            if resolved in seen_files:
                continue

            seen_files.add(resolved)

            suffix = path.suffix.lower()

            if suffix not in (
                ".json",
                ".jsonl",
                ".txt",
            ):
                continue

            try:
                text = path.read_text(
                    encoding="utf-8",
                    errors="replace",
                )
            except Exception:
                continue

            if not TV_ARTIFACT_MARKER_RE.search(
                text
            ):
                continue

            objects: List[Any] = []

            if suffix == ".jsonl":
                for line in text.splitlines():
                    line = line.strip()

                    if not line:
                        continue

                    try:
                        objects.append(
                            json.loads(line)
                        )
                    except Exception:
                        continue

            elif suffix == ".json":
                try:
                    objects.append(
                        json.loads(text)
                    )
                except Exception:
                    continue

            else:
                for line in text.splitlines():
                    if TV_ARTIFACT_MARKER_RE.search(
                        line
                    ):
                        objects.append({
                            "marker": line,
                            "name": line,
                        })

            for obj in objects:
                for item, marked in _walk_artifact_objects(
                    obj
                ):
                    if not marked:
                        continue

                    if not isinstance(item, dict):
                        continue

                    name = _first_string(
                        item,
                        (
                            "name",
                            "title",
                            "channel_name",
                            "channel",
                            "display_name",
                        ),
                    )

                    video_id = _first_string(
                        item,
                        (
                            "video_id",
                            "videoId",
                            "id",
                            "rutube_id",
                        ),
                    )

                    if not video_id:
                        for key in (
                            "url",
                            "link",
                            "href",
                            "video_url",
                        ):
                            candidate = item.get(key)

                            extracted = (
                                _extract_video_id_from_text(
                                    candidate
                                )
                            )

                            if extracted:
                                video_id = extracted
                                break

                    if not name and not video_id:
                        continue

                    record = dict(item)

                    if name:
                        record["name"] = name

                    if video_id:
                        record["video_id"] = video_id

                    record["tv_marker"] = (
                        "*Телеканалы*"
                    )

                    record.setdefault(
                        "discovered_from",
                        "workflow_artifact",
                    )

                    cards.append(record)

    logging.info(
        "ARTIFACT TV CARDS RAW: %d",
        len(cards),
    )

    return cards


def _unique_tv_cards(
    cards: List[Dict[str, Any]],
    target: int = TV_TARGET_COUNT,
) -> List[Dict[str, Any]]:
    """
    Build a finite set of unique real TV channels.

    Identity is based on video_id when available, otherwise
    on normalized channel name.
    """
    result: List[Dict[str, Any]] = []
    seen = set()

    for card in cards:
        if not isinstance(card, dict):
            continue

        video_id = str(
            card.get("video_id") or ""
        ).strip()

        name = str(
            card.get("name")
            or card.get("title")
            or card.get("channel_name")
            or ""
        ).strip()

        if video_id:
            identity = (
                "id:" +
                video_id.lower()
            )
        else:
            normalized = _normalize_tv_identity(
                name
            )

            if not normalized:
                continue

            identity = (
                "name:" +
                normalized
            )

        if identity in seen:
            continue

        seen.add(identity)

        item = dict(card)

        item["tv_marker"] = (
            "*Телеканалы*"
        )

        item["channel_identity"] = identity

        result.append(item)

        if len(result) >= target:
            break

    return result


def save_cumulative_tv_cards(
    cards: List[Dict[str, Any]],
    output_dir: Path,
) -> Path:
    """
    Save the cumulative TV-card set for future Workflow runs.
    """
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    path = (
        output_dir /
        "rutube_tv_cumulative_cards.json"
    )

    payload = {
        "schema": (
            "RutubeTVCumulativeCards"
        ),
        "marker": (
            "*Телеканалы*"
        ),
        "generated_at": (
            datetime.now(
                timezone.utc
            ).isoformat()
        ),
        "count": len(cards),
        "channels": cards,
    }

    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logging.info(
        "CUMULATIVE TV CARDS SAVED: %s count=%d",
        path,
        len(cards),
    )

    return path


# ============================================================
# RUTUBE SCRAPER
# ============================================================

class RutubeScraper:

    def __init__(
        self,
        output_dir: str = OUTPUT_DIR,
        timeout: int = 25,
    ):
        self.output_dir = Path(
            output_dir
        )

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.timeout = timeout

        self.session = requests.Session()

        self.request_stats: List[
            RequestStat
        ] = []

        self.started_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        self.finished_at = None

        self.endpoints = {
            "login":
                "https://rutube.ru/api/accounts/login/",
            "visitor":
                "https://rutube.ru/api/accounts/visitor/",
            "video":
                "https://rutube.ru/api/video/",
            "award":
                "https://rutube.ru/api/video/award/",
            "balancer":
                "https://rutube.ru/api/play/options/",
            "hls_api":
                "https://rutube.ru/api/play/options",
        }

        self._configure_session()

    # ========================================================
    # SESSION
    # ========================================================

    def _configure_session(
        self,
    ) -> None:

        self.session.headers.update({
            "User-Agent":
                M3U_USER_AGENT,
            "Accept":
                "*/*",
            "Accept-Language":
                "ru-RU,ru;q=0.9,en;q=0.8",
            "Connection":
                "keep-alive",
        })

        if Retry is not None:
            retry = Retry(
                total=3,
                connect=3,
                read=3,
                backoff_factor=0.3,
                status_forcelist=(
                    429,
                    500,
                    502,
                    503,
                    504,
                ),
                allowed_methods=False,
            )

            adapter = HTTPAdapter(
                max_retries=retry,
                pool_connections=32,
                pool_maxsize=32,
            )

            self.session.mount(
                "http://",
                adapter,
            )

            self.session.mount(
                "https://",
                adapter,
            )

    # ========================================================
    # REQUEST
    # ========================================================

    def request(
        self,
        method: str,
        url: str,
        *,
        name: str = "request",
        **kwargs,
    ) -> requests.Response:

        started = time.perf_counter()

        stat = RequestStat(
            name=name,
            method=method.upper(),
            url=self._safe_url(url),
        )

        try:
            response = self.session.request(
                method,
                url,
                timeout=kwargs.pop(
                    "timeout",
                    self.timeout,
                ),
                **kwargs,
            )

            elapsed = (
                time.perf_counter()
                - started
            ) * 1000

            stat.status = (
                response.status_code
            )

            stat.ok = response.ok

            stat.duration_ms = round(
                elapsed,
                2,
            )

            stat.response_size = len(
                response.content
            )

            self.request_stats.append(
                stat
            )

            return response

        except Exception as exc:
            elapsed = (
                time.perf_counter()
                - started
            ) * 1000

            stat.duration_ms = round(
                elapsed,
                2,
            )

            stat.error = str(exc)

            self.request_stats.append(
                stat
            )

            raise

    # ========================================================
    # SAFE URL
    # ========================================================

    @staticmethod
    def _safe_url(
        url: str,
    ) -> str:

        try:
            parsed = urlparse(
                str(url)
            )

            if not parsed.query:
                return str(url)

            query = parse_qs(
                parsed.query,
                keep_blank_values=True,
            )

            for secret in (
                "token",
                "access_token",
                "authorization",
                "password",
                "passwd",
            ):
                if secret in query:
                    query[secret] = [
                        "***"
                    ]

            return urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                urlencode(
                    query,
                    doseq=True,
                ),
                parsed.fragment,
            ))

        except Exception:
            return str(url)

    # ========================================================
    # VIDEO
    # ========================================================

    def video(
        self,
        video_id: str,
    ) -> VideoInfo:

        url = (
            f"{self.endpoints['video']}"
            f"{video_id}/"
        )

        response = self.request(
            "GET",
            url,
            name="video",
        )

        response.raise_for_status()

        data = response.json()

        title = (
            data.get("title")
            or data.get("name")
        )

        description = (
            data.get("description")
        )

        duration = data.get(
            "duration"
        )

        author_data = (
            data.get("author")
            or {}
        )

        if isinstance(
            author_data,
            dict,
        ):
            author = (
                author_data.get("name")
                or author_data.get("title")
            )

            author_id = (
                author_data.get("id")
            )
        else:
            author = None
            author_id = None

        return VideoInfo(
            requested_id=str(
                video_id
            ),
            internal_id=str(
                data.get("id")
                or video_id
            ),
            title=title,
            description=description,
            duration=duration,
            author=author,
            author_id=author_id,
            category=(
                data.get("category")
                if isinstance(
                    data.get("category"),
                    str,
                )
                else None
            ),
            created_at=data.get(
                "created_at"
            ),
            published_at=data.get(
                "published_at"
            ),
            raw=data,
        )

    # ========================================================
    # LOGIN
    # ========================================================

    def login(
        self,
        phone: str,
        password: str,
    ) -> Dict[str, Any]:

        response = self.request(
            "POST",
            self.endpoints["login"],
            name="login",
            json={
                "phone": phone,
                "password": password,
            },
        )

        response.raise_for_status()

        return response.json()

    # ========================================================
    # VISITOR
    # ========================================================

    def visitor(
        self,
    ) -> Dict[str, Any]:

        response = self.request(
            "GET",
            self.endpoints["visitor"],
            name="visitor",
        )

        response.raise_for_status()

        return response.json()

    # ========================================================
    # AWARD
    # ========================================================

    def award(
        self,
        video_id: str,
    ) -> Dict[str, Any]:

        response = self.request(
            "GET",
            self.endpoints["award"],
            name="award",
            params={
                "video_id":
                    video_id,
            },
        )

        response.raise_for_status()

        data = response.json()

        if not isinstance(
            data,
            dict,
        ):
            return {
                "award":
                    data,
            }

        return data

    # ========================================================
    # BALANCER
    # ========================================================

    def get_balancer(
        self,
        video_id: str,
        award: Any = None,
    ) -> Dict[str, Any]:

        params = {}

        if award is not None:
            params["award"] = award

        url = (
            f"{self.endpoints['balancer']}"
            f"{video_id}/"
        )

        response = self.request(
            "GET",
            url,
            name="balancer",
            params=params,
        )

        response.raise_for_status()

        data = response.json()

        if not isinstance(
            data,
            dict,
        ):
            raise RutubeScrapperError(
                "Balancer response is not an object"
            )

        return data

    # ========================================================
    # PLAY OPTIONS
    # ========================================================

    def get_live_options(
        self,
        video_id: str,
    ) -> Dict[str, Any]:

        url = (
            f"{self.endpoints['hls_api']}"
            f"/{video_id}/"
        )

        response = self.request(
            "GET",
            url,
            name="live_options",
            params={
                "format": "json",
                "no_404": "true",
            },
        )

        response.raise_for_status()

        data = response.json()

        if not isinstance(
            data,
            dict,
        ):
            raise RutubeScrapperError(
                "play/options response is not an object"
            )

        return data

    # ========================================================
    # RUTUBE BALANCER HLS
    # ========================================================

    def get_live_streams(
        self,
        video_id: str,
    ) -> List[str]:

        """
        Получить ВСЕ HLS URL, выданные Rutube через balancer.

        Цепочка строго такая:
            Discovery карточек -> video_id -> play/options ->
            https://bl.rutube.ru/livestream/*.m3u8

        Важно: bl.rutube.ru/livestream не используется как каталог.
        Конкретные подписанные URL балансировщика появляются в ответе
        play/options для уже найденного video_id.

        Берём не один заранее известный поток и не одно поле API,
        а все встреченные в ответе URL вида
        bl.rutube.ru/livestream/*.m3u8.

        Никакого выбора "лучшего" потока и никакого dedupe здесь нет.
        """

        data = self.get_live_options(
            video_id
        )

        result: List[str] = []

        self._collect_balancer_hls_urls(
            data,
            result,
        )

        logging.info(
            "RUTUBE BALANCER: video_id=%s streams=%d",
            video_id,
            len(result),
        )

        return result

    @staticmethod
    def _is_rutube_balancer_hls(
        url: str,
    ) -> bool:

        value = str(
            url or ""
        ).strip()

        base = (
            value
            .split(
                "?",
                1,
            )[0]
            .lower()
        )

        return (
            base.startswith(
                "https://bl.rutube.ru/livestream/"
            )
            and base.endswith(
                ".m3u8"
            )
        )

    @classmethod
    def _collect_balancer_hls_urls(
        cls,
        value: Any,
        result: List[str],
    ) -> None:

        """
        Рекурсивно забирает ВСЕ
        bl.rutube.ru/livestream/*.m3u8.

        Порядок ответа сохраняется.
        Повторяющиеся URL намеренно не удаляются:
        это сборщик, а не дедупликатор.
        """

        if isinstance(
            value,
            str,
        ):

            url = value.strip()

            if cls._is_rutube_balancer_hls(
                url
            ):
                result.append(
                    url
                )

            return

        if isinstance(
            value,
            dict,
        ):

            for child in value.values():
                cls._collect_balancer_hls_urls(
                    child,
                    result,
                )

            return

        if isinstance(
            value,
            (list, tuple),
        ):

            for child in value:
                cls._collect_balancer_hls_urls(
                    child,
                    result,
                )

    # ========================================================
    # MASTER M3U8
    # ========================================================

    def get_m3u8(
        self,
        url: str,
    ) -> str:

        response = self.request(
            "GET",
            url,
            name="m3u8",
        )

        response.raise_for_status()

        return response.text

    # ========================================================
    # M3U8 PARSER
    # ========================================================

    @staticmethod
    def parse_m3u8(
        text: str,
    ) -> List[StreamInfo]:

        streams: List[
            StreamInfo
        ] = []

        lines = [
            line.strip()
            for line
            in text.splitlines()
        ]

        current_attributes = {}

        for line in lines:

            if line.startswith(
                "#EXT-X-STREAM-INF:"
            ):

                raw = line.split(
                    ":",
                    1,
                )[1]

                attributes = {}

                for part in re.split(
                    r',(?=[A-Z0-9-]+=)',
                    raw,
                ):

                    if "=" not in part:
                        continue

                    key, value = (
                        part.split(
                            "=",
                            1,
                        )
                    )

                    value = value.strip()

                    if (
                        len(value) >= 2
                        and value[0] == '"'
                        and value[-1] == '"'
                    ):
                        value = value[1:-1]

                    attributes[
                        key
                    ] = value

                current_attributes = (
                    attributes
                )

                continue

            if (
                line
                and not line.startswith("#")
                and current_attributes
            ):

                resolution = (
                    current_attributes.get(
                        "RESOLUTION"
                    )
                )

                width = None
                height = None

                if resolution:
                    match = re.match(
                        r"(\d+)x(\d+)",
                        resolution,
                    )

                    if match:
                        width = int(
                            match.group(1)
                        )
                        height = int(
                            match.group(2)
                        )

                bandwidth = None

                bandwidth_raw = (
                    current_attributes.get(
                        "BANDWIDTH"
                    )
                )

                if bandwidth_raw:
                    try:
                        bandwidth = int(
                            bandwidth_raw
                        )
                    except Exception:
                        bandwidth = None

                average_bandwidth = None

                average_raw = (
                    current_attributes.get(
                        "AVERAGE-BANDWIDTH"
                    )
                )

                if average_raw:
                    try:
                        average_bandwidth = int(
                            average_raw
                        )
                    except Exception:
                        average_bandwidth = None

                frame_rate = None

                frame_rate_raw = (
                    current_attributes.get(
                        "FRAME-RATE"
                    )
                )

                if frame_rate_raw:
                    try:
                        frame_rate = float(
                            frame_rate_raw
                        )
                    except Exception:
                        frame_rate = None

                quality_score = (
                    (width or 0)
                    * (height or 0)
                )

                stream = StreamInfo(
                    resolution=resolution,
                    width=width,
                    height=height,
                    bandwidth=bandwidth,
                    bandwidth_kbps=(
                        bandwidth / 1000
                        if bandwidth
                        else None
                    ),
                    average_bandwidth=(
                        average_bandwidth
                    ),
                    codecs=(
                        current_attributes.get(
                            "CODECS"
                        )
                    ),
                    mime_type=(
                        current_attributes.get(
                            "TYPE"
                        )
                    ),
                    frame_rate=frame_rate,
                    audio=(
                        current_attributes.get(
                            "AUDIO"
                        )
                    ),
                    video=(
                        current_attributes.get(
                            "VIDEO"
                        )
                    ),
                    url=line,
                    protocol="HLS",
                    url_hash=hashlib.sha256(
                        line.encode(
                            "utf-8",
                            errors="replace",
                        )
                    ).hexdigest(),
                    quality_score=float(
                        quality_score
                    ),
                    source_index=len(
                        streams
                    ),
                    raw_attributes=dict(
                        current_attributes
                    ),
                )

                streams.append(
                    stream
                )

                current_attributes = {}

        return streams

    # ========================================================
    # STREAM TEST
    # ========================================================

    def test_stream(
        self,
        stream: StreamInfo,
    ) -> StreamInfo:

        started = time.perf_counter()

        try:
            response = self.session.get(
                stream.url,
                timeout=10,
                stream=True,
                headers={
                    "User-Agent":
                        M3U_USER_AGENT,
                    "Accept":
                        "*/*",
                },
            )

            elapsed = (
                time.perf_counter()
                - started
            ) * 1000

            stream.http_status = (
                response.status_code
            )

            stream.alive = (
                response.ok
            )

            stream.content_type = (
                response.headers.get(
                    "Content-Type"
                )
            )

            stream.response_time_ms = (
                round(
                    elapsed,
                    2,
                )
            )

            total = 0

            try:
                for chunk in response.iter_content(
                    chunk_size=8192
                ):
                    if chunk:
                        total += len(
                            chunk
                        )

                    if total >= 65536:
                        break

            finally:
                response.close()

            stream.response_size = total

        except Exception:
            stream.alive = False

        return stream

    # ========================================================
    # SINGLE VIDEO RUN
    # ========================================================

    def run_single_video(
        self,
        video_id: str,
        phone: str,
        password: str,
        test_streams: bool = True,
    ) -> Dict[str, Any]:

        self.started_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        video = self.video_after_login(
            video_id,
            phone,
            password,
        )

        award_data = self.award(
            video.internal_id
        )

        award_value = (
            award_data["award"]
        )

        balancer = self.get_balancer(
            video.internal_id,
            award_value,
        )

        m3u8_url = (
            balancer["m3u8_url"]
        )

        m3u8_text = self.get_m3u8(
            m3u8_url
        )

        streams = self.parse_m3u8(
            m3u8_text
        )

        if test_streams:

            for stream in streams:
                self.test_stream(
                    stream
                )

        self.finished_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )

        return self.build_report(
            video=video,
            award_data=award_data,
            balancer=balancer,
            m3u8_url=m3u8_url,
            m3u8_text=m3u8_text,
            streams=streams,
        )

    # ========================================================
    # LOGIN + VIDEO
    # ========================================================

    def video_after_login(
        self,
        video_id: str,
        phone: str,
        password: str,
    ) -> VideoInfo:

        self.login(
            phone,
            password,
        )

        return self.video(
            video_id
        )

    # ========================================================
    # REPORT
    # ========================================================

    def build_report(
        self,
        video: VideoInfo,
        award_data: Dict[str, Any],
        balancer: Dict[str, Any],
        m3u8_url: str,
        m3u8_text: str,
        streams: List[StreamInfo],
    ) -> Dict[str, Any]:

        return {
            "schema": {
                "name":
                    "RutubeMLVideoReport",

                "version":
                    VERSION,
            },

            "meta": {
                "generated_at":
                    datetime.now(
                        timezone.utc
                    ).isoformat(),

                "started_at":
                    self.started_at,

                "finished_at":
                    self.finished_at,

                "python_version":
                    sys.version,

                "platform":
                    sys.platform,
            },

            "video":
                asdict(video),

            "stream_source": {
                "balancer_url":
                    self._safe_url(
                        m3u8_url
                    ),

                "m3u8_sha256":
                    hashlib.sha256(
                        m3u8_text.encode(
                            "utf-8",
                            errors="replace",
                        )
                    ).hexdigest(),

                "line_count":
                    len(
                        m3u8_text.splitlines()
                    ),
            },

            "streams": [
                asdict(stream)
                for stream in streams
            ],

            "statistics":
                self.make_statistics(
                    streams
                ),

            "ml":
                self.make_ml_dataset(
                    video,
                    streams,
                ),

            "http": {
                "requests": [
                    asdict(stat)
                    for stat
                    in self.request_stats
                ]
            },

            "visitor":
                award_data.get(
                    "visitor",
                    {},
                ),

            "balancer":
                self._sanitize_balancer(
                    balancer
                ),
        }

    # ========================================================
    # STATISTICS
    # ========================================================

    @staticmethod
    def make_statistics(
        streams: List[StreamInfo],
    ) -> Dict[str, Any]:

        alive = [
            s for s in streams
            if s.alive is True
        ]

        dead = [
            s for s in streams
            if s.alive is False
        ]

        resolutions = {}

        for stream in streams:

            key = (
                stream.resolution
                or "unknown"
            )

            resolutions[key] = (
                resolutions.get(
                    key,
                    0,
                ) + 1
            )

        best = None

        if streams:

            best = max(
                streams,
                key=lambda s:
                    s.quality_score,
            )

        return {
            "total_streams":
                len(streams),

            "alive_streams":
                len(alive),

            "dead_streams":
                len(dead),

            "availability_percent":
                round(
                    len(alive)
                    / len(streams)
                    * 100,
                    2,
                )
                if streams
                else 0,

            "resolutions":
                resolutions,

            "best_stream":
                asdict(best)
                if best
                else None,

            "bandwidth_min":
                min(
                    (
                        s.bandwidth
                        for s in streams
                        if s.bandwidth
                    ),
                    default=None,
                ),

            "bandwidth_max":
                max(
                    (
                        s.bandwidth
                        for s in streams
                        if s.bandwidth
                    ),
                    default=None,
                ),
        }

    # ========================================================
    # ML DATASET
    # ========================================================

    @staticmethod
    def make_ml_dataset(
        video: VideoInfo,
        streams: List[StreamInfo],
    ) -> Dict[str, Any]:

        records = []

        for stream in streams:

            features = {
                "video_id":
                    video.internal_id,

                "stream_index":
                    stream.source_index,

                "width":
                    stream.width or 0,

                "height":
                    stream.height or 0,

                "pixels":
                    (
                        (stream.width or 0)
                        * (stream.height or 0)
                    ),

                "bandwidth":
                    stream.bandwidth or 0,

                "bandwidth_kbps":
                    stream.bandwidth_kbps or 0,

                "average_bandwidth":
                    stream.average_bandwidth
                    or 0,

                "frame_rate":
                    stream.frame_rate or 0,

                "alive":
                    1
                    if stream.alive is True
                    else 0,

                "http_status":
                    stream.http_status or 0,

                "response_time_ms":
                    stream.response_time_ms
                    or 0,

                "quality_score":
                    stream.quality_score,

                "has_audio":
                    1
                    if stream.audio
                    else 0,

                "has_video":
                    1
                    if stream.video
                    else 0,

                "has_codecs":
                    1
                    if stream.codecs
                    else 0,
            }

            records.append({
                "features":
                    features,

                "target": {
                    "alive":
                        stream.alive,

                    "quality_score":
                        stream.quality_score,
                },

                "metadata": {
                    "resolution":
                        stream.resolution,

                    "codecs":
                        stream.codecs,

                    "url_hash":
                        stream.url_hash,
                },
            })

        return {
            "feature_schema": {
                "width":
                    "integer",

                "height":
                    "integer",

                "pixels":
                    "integer",

                "bandwidth":
                    "integer",

                "bandwidth_kbps":
                    "float",

                "average_bandwidth":
                    "integer",

                "frame_rate":
                    "float",

                "alive":
                    "binary",

                "http_status":
                    "integer",

                "response_time_ms":
                    "float",

                "quality_score":
                    "float",

                "has_audio":
                    "binary",

                "has_video":
                    "binary",

                "has_codecs":
                    "binary",
            },

            "records":
                records,
        }

    # ========================================================
    # LEGACY M3U PLAYLIST
    # ========================================================

    @staticmethod
    def make_m3u(
        video: VideoInfo,
        streams: List[StreamInfo],
        only_alive: bool = False,
    ) -> str:

        output = [
            "#EXTM3U",
        ]

        for stream in streams:

            if (
                only_alive
                and stream.alive is not True
            ):
                continue

            title = (
                video.title
                or video.internal_id
            )

            output.append(
                (
                    '#EXTINF:-1 '
                    f'tvg-id="{video.internal_id}" '
                    f'tvg-name="{title}",'
                    f'{title}'
                )
            )

            output.append(
                stream.url
            )

        return "\n".join(
            output
        )

    # ========================================================
    # BALANCER SANITIZER
    # ========================================================

    @classmethod
    def _sanitize_balancer(
        cls,
        value: Any,
    ) -> Any:

        if isinstance(
            value,
            str,
        ):
            return cls._safe_url(
                value
            )

        if isinstance(
            value,
            dict,
        ):
            return {
                key:
                    cls._sanitize_balancer(
                        child
                    )
                for key, child
                in value.items()
            }

        if isinstance(
            value,
            list,
        ):
            return [
                cls._sanitize_balancer(
                    child
                )
                for child in value
            ]

        return value


# ============================================================
# TV DISCOVERY METHODS
# ============================================================

def _extract_text(
    value: Any,
) -> str:

    if value is None:
        return ""

    if isinstance(
        value,
        str,
    ):
        return html.unescape(
            value
        ).strip()

    return str(
        value
    ).strip()


def _looks_like_technical_id(
    value: str,
) -> bool:

    text = (
        value
        .strip()
        .lower()
    )

    technical_tokens = (
        "tvfavorites",
        "history",
        "topic",
        "assets",
        "banner-",
        "autowidget",
        "feedsource",
    )

    return any(
        token in text
        for token in technical_tokens
    )


def _extract_live_video_id(
    value: Any,
) -> Optional[str]:

    if isinstance(
        value,
        dict,
    ):

        for key in (
            "video_id",
            "videoId",
            "rutube_id",
        ):

            candidate = value.get(
                key
            )

            if candidate:
                candidate = str(
                    candidate
                ).strip()

                if not _looks_like_technical_id(
                    candidate
                ):
                    return candidate

        for key in (
            "url",
            "href",
            "link",
            "video_url",
        ):

            candidate = value.get(
                key
            )

            result = (
                _extract_video_id_from_text(
                    candidate
                )
            )

            if result:
                return result

        for child in value.values():

            result = (
                _extract_live_video_id(
                    child
                )
            )

            if result:
                return result

        return None

    if isinstance(
        value,
        (list, tuple),
    ):

        for child in value:

            result = (
                _extract_live_video_id(
                    child
                )
            )

            if result:
                return result

    return _extract_video_id_from_text(
        value
    )


def _extract_channel_name(
    item: Dict[str, Any],
) -> str:

    for key in (
        "name",
        "title",
        "channel_name",
        "display_name",
        "label",
    ):

        value = item.get(
            key
        )

        text = _extract_text(
            value
        )

        if text:
            return text

    return ""


def _parse_tv_page(
    text: str,
    category: str = "",
    source_url: str = "",
) -> List[Dict[str, Any]]:

    cards: List[
        Dict[str, Any]
    ] = []

    seen_local = set()

    patterns = (
        r'href=["\']'
        r'([^"\']*/live/video/[^"\']+)'
        r'["\']',

        r'"url"\s*:\s*"'
        r'([^"]*/live/video/[^"]+)'
        r'"',

        r'"href"\s*:\s*"'
        r'([^"]*/live/video/[^"]+)'
        r'"',
    )

    urls = []

    for pattern in patterns:

        for match in re.finditer(
            pattern,
            text,
            re.IGNORECASE,
        ):

            value = (
                html.unescape(
                    match.group(1)
                )
            )

            value = value.replace(
                "\\/",
                "/",
            )

            urls.append(
                urljoin(
                    "https://rutube.ru",
                    value,
                )
            )

    for url in urls:

        video_id = (
            _extract_video_id_from_text(
                url
            )
        )

        if not video_id:
            continue

        if _looks_like_technical_id(
            video_id
        ):
            continue

        if video_id in seen_local:
            continue

        seen_local.add(
            video_id
        )

        name = ""

        pos = text.find(
            url
        )

        if pos >= 0:

            window = text[
                max(
                    0,
                    pos - 1200,
                ):
                min(
                    len(text),
                    pos + 1200,
                )
            ]

            title_match = re.search(
                r'"(?:title|name|channel_name)"'
                r'\s*:\s*"([^"]{2,200})"',
                window,
                re.IGNORECASE,
            )

            if title_match:
                name = html.unescape(
                    title_match.group(1)
                ).strip()

        if not name:
            name = (
                f"TV {video_id}"
            )

        cards.append({
            "name":
                name,

            "title":
                name,

            "video_id":
                video_id,

            "url":
                url,

            "category":
                category,

            "source_url":
                source_url,

            "tv_marker":
                "*Телеканалы*",
        })

    return cards


def _discover_missing_tv_cards_from_youtube(
    scraper: RutubeScraper,
    existing_cards: List[Dict[str, Any]],
    target: int = TV_TARGET_COUNT,
) -> List[Dict[str, Any]]:

    """
    Supplement missing TV cards using YouTube discovery.

    YouTube is used only to obtain additional channel-name seeds.
    Actual IPTV/HLS links are still resolved from Rutube.
    """

    cards = list(
        existing_cards
    )

    if len(cards) >= target:
        return cards

    seen_seeds = set()

    youtube_headers = {
        "User-Agent":
            M3U_USER_AGENT,
        "Accept-Language":
            "ru-RU,ru;q=0.9",
    }

    for query in YOUTUBE_DISCOVERY_QUERIES:

        if len(cards) >= target:
            break

        try:

            response = requests.get(
                "https://www.youtube.com/results",
                params={
                    "search_query":
                        query,
                },
                headers=youtube_headers,
                timeout=20,
            )

            if not response.ok:
                continue

            text = response.text

            title_patterns = (
                r'"title"\s*:\s*\{'
                r'\s*"runs"\s*:\s*\[\s*\{'
                r'\s*"text"\s*:\s*"([^"]+)"',

                r'"title"\s*:\s*\{'
                r'\s*"simpleText"\s*:\s*"([^"]+)"',
            )

            titles = []

            for pattern in title_patterns:

                for match in re.finditer(
                    pattern,
                    text,
                    re.IGNORECASE,
                ):

                    title = html.unescape(
                        match.group(1)
                    ).strip()

                    if (
                        title
                        and title not in titles
                    ):
                        titles.append(
                            title
                        )

            for title in titles:

                if len(cards) >= target:
                    break

                if len(
                    seen_seeds
                ) >= YOUTUBE_DISCOVERY_MAX_CANDIDATES:
                    break

                normalized = (
                    _normalize_tv_identity(
                        title
                    )
                )

                if (
                    not normalized
                    or normalized in seen_seeds
                ):
                    continue

                seen_seeds.add(
                    normalized
                )

                search_url = (
                    "https://rutube.ru/search/"
                )

                try:

                    search_response = (
                        scraper.request(
                            "GET",
                            search_url,
                            name="youtube_rutube_search",
                            params={
                                "query":
                                    title,
                            },
                            timeout=20,
                        )
                    )

                    if not search_response.ok:
                        continue

                    found = _parse_tv_page(
                        search_response.text,
                        category="YouTube supplemental",
                        source_url=search_response.url,
                    )

                    for card in found:

                        card["tv_marker"] = (
                            "*Телеканалы*"
                        )

                        card[
                            "discovered_from"
                        ] = (
                            "youtube->rutube"
                        )

                        cards.append(
                            card
                        )

                        unique = _unique_tv_cards(
                            cards,
                            target=target,
                        )

                        if len(unique) >= target:
                            return unique

                except Exception as exc:
                    logging.warning(
                        "YOUTUBE -> RUTUBE SEARCH FAILED: %s",
                        exc,
                    )

        except Exception as exc:
            logging.warning(
                "YOUTUBE DISCOVERY FAILED: %s",
                exc,
            )

    return _unique_tv_cards(
        cards,
        target=target,
    )


def discover_tv_catalog(
    scraper: RutubeScraper,
    output_dir: Path,
    target: int = TV_TARGET_COUNT,
) -> List[Dict[str, Any]]:

    """
    Финальный Discovery:

        1. Workflow artifacts
        2. *Телеканалы*
        3. unique TV cards
        4. YouTube supplemental search if needed
        5. stop at target
        6. only then resolve video_id -> streams
    """

    logging.info(
        "DISCOVERY START: TARGET=%d",
        target,
    )

    artifact_cards = (
        load_previous_tv_cards(
            output_dir
        )
    )

    unique_cards = _unique_tv_cards(
        artifact_cards,
        target=target,
    )

    logging.info(
        "DISCOVERY ARTIFACT UNIQUE TV: %d/%d",
        len(unique_cards),
        target,
    )

    if len(unique_cards) < target:

        unique_cards = (
            _discover_missing_tv_cards_from_youtube(
                scraper,
                unique_cards,
                target=target,
            )
        )

    unique_cards = _unique_tv_cards(
        unique_cards,
        target=target,
    )

    if len(unique_cards) < target:

        logging.warning(
            "DISCOVERY FINISHED BELOW TARGET: %d/%d",
            len(unique_cards),
            target,
        )

    else:

        logging.info(
            "DISCOVERY TARGET REACHED: %d/%d",
            len(unique_cards),
            target,
        )

    save_cumulative_tv_cards(
        unique_cards,
        output_dir,
    )

    return unique_cards


# ============================================================
# TV STREAM RESOLUTION
# ============================================================

def resolve_tv_card_streams(
    scraper: RutubeScraper,
    card: Dict[str, Any],
) -> Dict[str, Any]:

    result = dict(
        card
    )

    video_id = str(
        card.get(
            "video_id"
        )
        or ""
    ).strip()

    if not video_id:

        result.update({
            "status":
                "missing_video_id",

            "hls_streams":
                [],

            "stream_count":
                0,
        })

        return result

    try:

        streams = scraper.get_live_streams(
            video_id
        )

        result.update({
            "status":
                "ok"
                if streams
                else "no_streams",

            "hls_streams":
                streams,

            "stream_count":
                len(streams),

            "stream_source":
                "https://bl.rutube.ru/livestream/",
        })

    except Exception as exc:

        logging.error(
            "TV STREAM RESOLUTION FAILED: %s: %s",
            video_id,
            exc,
        )

        result.update({
            "status":
                "error",

            "error":
                str(exc),

            "hls_streams":
                [],

            "stream_count":
                0,
        })

    return result


# ============================================================
# TV M3U
# ============================================================

def make_tv_m3u(
    records: List[Dict[str, Any]],
) -> str:

    output = [
        "#EXTM3U",
    ]

    for record in records:

        name = (
            str(
                record.get(
                    "name"
                )
                or record.get(
                    "title"
                )
                or "Телеканал"
            ).strip()
        )

        category = (
            str(
                record.get(
                    "category"
                )
                or RUTUBE_GROUP
            ).strip()
        )

        video_id = (
            str(
                record.get(
                    "video_id"
                )
                or ""
            ).strip()
        )

        streams = (
            record.get(
                "hls_streams"
            )
            or []
        )

        if not isinstance(
            streams,
            list,
        ):
            continue

        for stream_index, stream_url in enumerate(
            streams,
            start=1,
        ):

            stream_url = str(
                stream_url
                or ""
            ).strip()

            if not stream_url:
                continue

            if not RutubeScraper._is_rutube_balancer_hls(
                stream_url
            ):
                continue

            output.append(
                (
                    '#EXTINF:-1 '
                    f'tvg-id="{video_id}" '
                    f'tvg-name="{name}" '
                    f'group-title="{category}",'
                    f'{name}'
                )
            )

            output.append(
                stream_url
            )

    return "\n".join(
        output
    ) + "\n"


# ============================================================
# TV JSON / JSONL / TXT
# ============================================================

def make_tv_json(
    records: List[Dict[str, Any]],
) -> Dict[str, Any]:

    total_streams = sum(
        int(
            record.get(
                "stream_count",
                0,
            )
            or 0
        )
        for record in records
    )

    return {
        "schema": {
            "name":
                "RutubeTVDiscovery",

            "version":
                VERSION,
        },

        "meta": {
            "generated_at":
                datetime.now(
                    timezone.utc
                ).isoformat(),

            "target":
                TV_TARGET_COUNT,

            "count":
                len(records),
        },

        "discovery": {
            "marker":
                "*Телеканалы*",

            "source_order": [
                "workflow_artifacts",
                "*Телеканалы*",
                "youtube",
                "rutube",
            ],

            "channel_deduplication":
                True,

            "stream_deduplication":
                False,

            "stream_source":
                "https://bl.rutube.ru/livestream/",
        },

        "statistics": {
            "channels":
                len(records),

            "streams":
                total_streams,

            "channels_with_streams":
                sum(
                    1
                    for record
                    in records
                    if record.get(
                        "stream_count",
                        0,
                    )
                ),
        },

        "channels":
            records,
    }


def make_tv_txt(
    records: List[Dict[str, Any]],
) -> str:

    lines = [
        "RUTUBE TV DISCOVERY",
        f"VERSION: {VERSION}",
        "",
        "DISCOVERY: UNIQUE TV CHANNELS / TARGET 400",
        "MARKER: *Телеканалы*",
        "STREAM SOURCE: https://bl.rutube.ru/livestream/",
        "",
    ]

    for index, record in enumerate(
        records,
        start=1,
    ):

        name = (
            str(
                record.get(
                    "name"
                )
                or record.get(
                    "title"
                )
                or "Телеканал"
            )
        )

        video_id = (
            str(
                record.get(
                    "video_id"
                )
                or ""
            )
        )

        category = (
            str(
                record.get(
                    "category"
                )
                or ""
            )
        )

        streams = (
            record.get(
                "hls_streams"
            )
            or []
        )

        lines.append(
            f"{index}. {name}"
        )

        lines.append(
            f"   video_id: {video_id}"
        )

        if category:
            lines.append(
                f"   category: {category}"
            )

        lines.append(
            f"   streams: {len(streams)}"
        )

        for stream_index, url in enumerate(
            streams,
            start=1,
        ):

            lines.append(
                f"   [{stream_index}] {url}"
            )

        lines.append("")

    return "\n".join(
        lines
    )


def save_tv_outputs(
    records: List[Dict[str, Any]],
    output_dir: Path,
) -> None:

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    m3u_path = (
        output_dir /
        M3U_FILENAME
    )

    json_path = (
        output_dir /
        JSON_FILENAME
    )

    jsonl_path = (
        output_dir /
        JSONL_FILENAME
    )

    txt_path = (
        output_dir /
        TXT_FILENAME
    )

    m3u_path.write_text(
        make_tv_m3u(
            records
        ),
        encoding="utf-8",
    )

    json_path.write_text(
        json.dumps(
            make_tv_json(
                records
            ),
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    with jsonl_path.open(
        "w",
        encoding="utf-8",
    ) as fh:

        for record in records:

            fh.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
            )

            fh.write("\n")

    txt_path.write_text(
        make_tv_txt(
            records
        ),
        encoding="utf-8",
    )

    logging.info(
        "TV OUTPUT SAVED: %s",
        m3u_path,
    )

    logging.info(
        "TV OUTPUT SAVED: %s",
        json_path,
    )

    logging.info(
        "TV OUTPUT SAVED: %s",
        jsonl_path,
    )

    logging.info(
        "TV OUTPUT SAVED: %s",
        txt_path,
    )


# ============================================================
# TV CATALOG SCRAPE
# ============================================================

def scrape_tv_catalog(
    scraper: RutubeScraper,
    cards: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    """
    После завершения Discovery последовательно получает
    ВСЕ balancer HLS URL для каждой выбранной карточки.

    Никакого ограничения количества stream URL внутри карточки нет.
    """

    results: List[
        Dict[str, Any]
    ] = []

    total = len(cards)

    logging.info(
        "TV STREAM STAGE START: cards=%d",
        total,
    )

    for index, card in enumerate(
        cards,
        start=1,
    ):

        name = (
            card.get(
                "name"
            )
            or card.get(
                "title"
            )
            or "Телеканал"
        )

        video_id = (
            card.get(
                "video_id"
            )
            or ""
        )

        logging.info(
            "TV [%d/%d] %s video_id=%s",
            index,
            total,
            name,
            video_id,
        )

        result = resolve_tv_card_streams(
            scraper,
            card,
        )

        results.append(
            result
        )

    logging.info(
        "TV STREAM STAGE FINISHED: records=%d",
        len(results),
    )

    return results


# ============================================================
# LOGGING
# ============================================================

def configure_logging(
    output_dir: Path,
) -> None:

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    log_path = (
        output_dir /
        "rutube_tv.log"
    )

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s "
            "[%(levelname)s] "
            "%(message)s"
        ),
        handlers=[
            logging.StreamHandler(
                sys.stdout
            ),
            logging.FileHandler(
                log_path,
                encoding="utf-8",
            ),
        ],
        force=True,
    )


# ============================================================
# AUTONOMOUS MODE
# ============================================================

def run_autonomous(
    output_dir: str = OUTPUT_DIR,
) -> Dict[str, Any]:

    output_path = Path(
        output_dir
    )

    configure_logging(
        output_path
    )

    logging.info(
        "============================================================"
    )

    logging.info(
        "RUTUBE TV AUTONOMOUS DISCOVERY"
    )

    logging.info(
        "VERSION: %s",
        VERSION,
    )

    logging.info(
        "DISCOVERY: WORKFLOW ARTIFACTS -> *Телеканалы* -> "
        "YOUTUBE -> RUTUBE"
    )

    logging.info(
        "TARGET TV CHANNELS: %d",
        TV_TARGET_COUNT,
    )

    logging.info(
        "STREAM SOURCE: https://bl.rutube.ru/livestream/"
    )

    logging.info(
        "============================================================"
    )

    scraper = RutubeScraper(
        output_dir=str(
            output_path
        )
    )

    # --------------------------------------------------------
    # FIRST: DISCOVERY
    # --------------------------------------------------------

    cards = discover_tv_catalog(
        scraper,
        output_path,
        target=TV_TARGET_COUNT,
    )

    logging.info(
        "FINAL DISCOVERY CARD COUNT: %d",
        len(cards),
    )

    # --------------------------------------------------------
    # ONLY AFTER DISCOVERY:
    # VIDEO ID -> PLAY OPTIONS -> BALANCER
    # --------------------------------------------------------

    records = scrape_tv_catalog(
        scraper,
        cards,
    )

    # --------------------------------------------------------
    # OUTPUT
    # --------------------------------------------------------

    save_tv_outputs(
        records,
        output_path,
    )

    total_streams = sum(
        int(
            record.get(
                "stream_count",
                0,
            )
            or 0
        )
        for record in records
    )

    report = {
        "schema": {
            "name":
                "RutubeTVAutonomousReport",

            "version":
                VERSION,
        },

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "discovery": {
            "target":
                TV_TARGET_COUNT,

            "cards":
                len(cards),

            "marker":
                "*Телеканалы*",

            "sources": [
                "workflow_artifacts",
                "youtube",
                "rutube",
            ],
        },

        "streams": {
            "total":
                total_streams,

            "source":
                "https://bl.rutube.ru/livestream/",
        },

        "channels":
            records,

        "http": {
            "requests": [
                asdict(stat)
                for stat
                in scraper.request_stats
            ],
        },
    }

    report_path = (
        output_path /
        "rutube_tv_autonomous_report.json"
    )

    report_path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    logging.info(
        "AUTONOMOUS REPORT: %s",
        report_path,
    )

    logging.info(
        "============================================================"
    )

    logging.info(
        "FINISHED: channels=%d streams=%d",
        len(records),
        total_streams,
    )

    logging.info(
        "============================================================"
    )

    return report


# ============================================================
# CLI
# ============================================================

def parse_args(
    argv: Optional[List[str]] = None,
) -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "Rutube scraper + autonomous TV discovery"
        )
    )

    parser.add_argument(
        "video_id",
        nargs="?",
        help=(
            "Legacy single Rutube video ID"
        ),
    )

    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help=(
            "Output directory"
        ),
    )

    parser.add_argument(
        "--phone",
        default=os.getenv(
            "RUTUBE_PHONE",
            "",
        ),
        help=(
            "Rutube phone for legacy mode"
        ),
    )

    parser.add_argument(
        "--password",
        default=os.getenv(
            "RUTUBE_PASSWORD",
            "",
        ),
        help=(
            "Rutube password for legacy mode"
        ),
    )

    parser.add_argument(
        "--no-test-streams",
        action="store_true",
        help=(
            "Do not test streams in legacy mode"
        ),
    )

    return parser.parse_args(
        argv
    )


def main(
    argv: Optional[List[str]] = None,
) -> int:

    args = parse_args(
        argv
    )

    if not args.video_id:

        run_autonomous(
            output_dir=args.output_dir
        )

        return 0

    output_dir = Path(
        args.output_dir
    )

    configure_logging(
        output_dir
    )

    if not args.phone:
        args.phone = input(
            "Rutube phone: "
        ).strip()

    if not args.password:
        args.password = input(
            "Rutube password: "
        )

    scraper = RutubeScraper(
        output_dir=str(
            output_dir
        )
    )

    try:

        report = scraper.run_single_video(
            args.video_id,
            args.phone,
            args.password,
            test_streams=(
                not args.no_test_streams
            ),
        )

    except Exception as exc:

        logging.exception(
            "LEGACY RUN FAILED: %s",
            exc,
        )

        return 1

    json_path = (
        output_dir /
        "rutube_video_report.json"
    )

    txt_path = (
        output_dir /
        "rutube_video_report.txt"
    )

    m3u_path = (
        output_dir /
        "rutube_video.m3u"
    )

    json_path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    video_data = report.get(
        "video",
        {},
    )

    streams = [
        StreamInfo(
            **{
                key: value
                for key, value
                in stream.items()
                if key in {
                    field_name
                    for field_name
                    in StreamInfo.__dataclass_fields__
                }
            }
        )
        for stream
        in report.get(
            "streams",
            [],
        )
    ]

    video = VideoInfo(
        **{
            key: value
            for key, value
            in video_data.items()
            if key in {
                field_name
                for field_name
                in VideoInfo.__dataclass_fields__
            }
        }
    )

    m3u_path.write_text(
        RutubeScraper.make_m3u(
            video,
            streams,
        ),
        encoding="utf-8",
    )

    txt_lines = [
        "RUTUBE VIDEO REPORT",
        "",
        f"ID: {video.internal_id}",
        f"TITLE: {video.title or ''}",
        "",
        "STREAMS:",
    ]

    for index, stream in enumerate(
        streams,
        start=1,
    ):

        txt_lines.append(
            (
                f"{index}. "
                f"{stream.resolution or 'unknown'} "
                f"{stream.url}"
            )
        )

    txt_path.write_text(
        "\n".join(
            txt_lines
        ),
        encoding="utf-8",
    )

    logging.info(
        "LEGACY JSON: %s",
        json_path,
    )

    logging.info(
        "LEGACY M3U: %s",
        m3u_path,
    )

    logging.info(
        "LEGACY TXT: %s",
        txt_path,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )