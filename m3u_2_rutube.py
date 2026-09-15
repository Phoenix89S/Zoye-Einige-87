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
       - получает live_streams.hls;
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


    4. VIDEO_ID → live_streams.hls
       - get_live_options(video_id)
       - live_streams.hls (все ссылки)


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


VERSION = "2.2.0-TV-SHELF-CARDS"


# ============================================================
# AUTONOMOUS TV CONFIGURATION
# ============================================================


# ЕДИНСТВЕННЫЙ основной источник.
#
# Пользователь может заменить эту строку на другую страницу
# RUTUBE TV, если понадобится.
DISCOVERY_SOURCE_URL = "https://rutube.ru/feeds/live/"


# Дополнительный официальный каталог телепрограмм.
#
# Он используется как второй discovery-source.
TV_PROGRAM_SOURCE_URL = (
    "https://rutube.ru/feeds/live/tvprogramm/"
)


# Общий каталог.
#
# ВАЖНО:
# Эти URL НЕ являются единственным источником категорий.
# Скрипт сначала пытается обнаружить реальные ссылки
# категорий непосредственно из HTML RUTUBE.
#
# Эти адреса используются как fallback.
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


# Максимальное число страниц одной категории.
#
# Можно увеличить через:
#
# RUTUBE_MAX_PAGES=200
#
DEFAULT_MAX_PAGES = 100


# Если страница содержит хотя бы один найденный video ID,
# продолжаем постраничный обход.
#
# Останавливаемся после MAX_EMPTY_PAGES подряд пустых страниц.
DEFAULT_MAX_EMPTY_PAGES = 2


# Общая группа.
RUTUBE_GROUP = "Рутуб кабельная"


# EPG.
M3U_EPG_URL = "https://iptvx.one/EPG"


# User-Agent из рабочего M3U пользователя.
M3U_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


# Output.
OUTPUT_DIR = "rutube_output"


M3U_FILENAME = "rutube_tv.m3u"
JSON_FILENAME = "rutube_tv.json"
JSONL_FILENAME = "rutube_tv.jsonl"
TXT_FILENAME = "rutube_tv.txt"


DISCOVERY_HTML_FILENAME = "rutube_tv_discovery_debug.html"


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
# SCRAPER
# ============================================================


class RutubeScrapper:


    def __init__(
        self,
        timeout: tuple = (10, 30),
        retries: int = 3,
        verify_ssl: bool = True,
        user_agent: Optional[str] = None,
    ):
        self.endpoints = {
            "login_api":
                "https://pass.rutube.ru/api/accounts/phone/login/",


            "login_social_api":
                "https://rutube.ru/social/auth/rupass/"
                "?callback_path=/social/login/rupass/",


            "video_api":
                "https://rutube.ru/api/video",


            "visitor_api":
                "https://rutube.ru/api/accounts/visitor/",


            "ad_api":
                "https://mtr.rutube.ru/api/v3/interactive",


            "hls_api":
                "https://rutube.ru/api/play/options",
        }


        self.timeout = timeout
        self.verify_ssl = verify_ssl


        self.session = requests.Session()


        self.session.headers.update({
            "User-Agent": user_agent or (
                "Rutube-Full-Scraper/"
                + VERSION
            ),
            "Accept": "*/*",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Referer": "https://rutube.ru/",
        })


        self.request_stats: List[RequestStat] = []


        self.started_at = None
        self.finished_at = None


        self._configure_retry(retries)


    # ========================================================
    # HTTP
    # ========================================================


    def _configure_retry(
        self,
        retries: int,
    ):


        if Retry is None:
            return


        retry = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=0.5,
            status_forcelist=(
                429,
                500,
                502,
                503,
                504,
            ),
            allowed_methods=frozenset({
                "GET",
                "HEAD",
                "OPTIONS",
            }),
            raise_on_status=False,
        )


        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=32,
            pool_maxsize=32,
        )


        self.session.mount(
            "https://",
            adapter,
        )


        self.session.mount(
            "http://",
            adapter,
        )


    def _request(
        self,
        method: str,
        url: str,
        name: str,
        **kwargs,
    ) -> requests.Response:


        started = time.perf_counter()


        stat = RequestStat(
            name=name,
            method=method,
            url=self._safe_url(url),
        )


        try:


            response = self.session.request(
                method,
                url,
                timeout=self.timeout,
                verify=self.verify_ssl,
                **kwargs,
            )


            elapsed = (
                time.perf_counter()
                - started
            ) * 1000


            stat.status = response.status_code
            stat.ok = response.ok
            stat.duration_ms = round(
                elapsed,
                3,
            )


            try:
                stat.response_size = len(
                    response.content
                )
            except Exception:
                pass


            self.request_stats.append(
                stat
            )


            response.raise_for_status()


            return response


        except Exception as exc:


            elapsed = (
                time.perf_counter()
                - started
            ) * 1000


            stat.duration_ms = round(
                elapsed,
                3,
            )


            stat.error = str(exc)


            self.request_stats.append(
                stat
            )


            raise RutubeScrapperError(
                f"{name}: {exc}"
            ) from exc


    # ========================================================
    # SECURITY
    # ========================================================


    @staticmethod
    def _safe_url(
        url: str,
    ) -> str:


        try:


            parsed = urlparse(url)


            sensitive = {
                "club_token",
                "token",
                "password",
                "passwd",
                "access_token",
                "refresh_token",
            }


            query = parse_qs(
                parsed.query,
                keep_blank_values=True,
            )


            for key in list(query):


                if key.lower() in sensitive:
                    query[key] = [
                        "<REDACTED>"
                    ]


            safe_query = urlencode(
                query,
                doseq=True,
            )


            return urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                safe_query,
                parsed.fragment,
            ))


        except Exception:


            return "<URL>"


    # ============================================================
    # LOGIN
    # ============================================================


    def login(
        self,
        phone: str,
        password: str,
    ) -> Dict[str, Any]:


        if (
            not phone.strip()
            or not password.strip()
        ):
            raise RutubeScrapperError(
                "You must enter Rutube LiST credentials"
            )


        response = self._request(
            "POST",
            self.endpoints["login_api"],
            "login",
            json={
                "phone": phone,
                "password": password,
            },
        )


        result = self._json(
            response,
            "login",
        )


        if result.get(
            "success"
        ) is not True:


            raise RutubeScrapperError(
                "Invalid credentials"
            )


        self._request(
            "GET",
            self.endpoints["login_social_api"],
            "social_cookie_init",
        )


        return result


    # ========================================================
    # VIDEO
    # ========================================================


    def video(
        self,
        video_id: str,
    ) -> VideoInfo:


        url = (
            f"{self.endpoints['video_api']}/"
            f"{video_id}"
        )


        response = self._request(
            "GET",
            url,
            "video",
        )


        data = self._json(
            response,
            "video",
        )


        internal_id = data.get("id")


        if internal_id is None:


            raise RutubeScrapperError(
                "Internal video id was not found"
            )


        return VideoInfo(
            requested_id=str(video_id),
            internal_id=str(internal_id),
            title=data.get("title"),
            description=data.get("description"),
            duration=self._to_float(
                data.get("duration")
            ),
            author=self._extract_author(
                data
            ),
            author_id=self._extract_author_id(
                data
            ),
            category=self._extract_category(
                data
            ),
            created_at=(
                data.get("created_ts")
                or data.get("created_at")
            ),
            published_at=(
                data.get("publication_date")
                or data.get("published_at")
            ),
            raw=data,
        )


    # ========================================================
    # VISITOR
    # ========================================================


    def visitor(
        self,
    ) -> Dict[str, Any]:


        response = self._request(
            "GET",
            self.endpoints["visitor_api"],
            "visitor",
        )


        return self._json(
            response,
            "visitor",
        )


    # ========================================================
    # AWARD
    # ========================================================


    def award(
        self,
        internal_video_id: str,
    ) -> Dict[str, Any]:


        visitor_data = self.visitor()


        club_params = visitor_data.get(
            "club_params_encrypted"
        )


        if not club_params:


            raise RutubeScrapperError(
                "club_params_encrypted was not found"
            )


        params = {
            "video_id":
                internal_video_id,
        }


        ad_url = (
            self.endpoints["ad_api"]
            + "?"
            + club_params.lstrip("?")
            + "&"
            + urlencode(params)
        )


        response = self._request(
            "GET",
            ad_url,
            "award",
        )


        data = self._json(
            response,
            "award",
        )


        award_value = data.get(
            "award"
        )


        if not award_value:


            raise RutubeScrapperError(
                "Award token was not returned"
            )


        return {
            "award":
                award_value,


            "visitor":
                self._sanitize_visitor(
                    visitor_data
                ),
        }


    # ========================================================
    # HLS BALANCER
    # ========================================================


    def get_balancer(
        self,
        internal_video_id: str,
        award: str,
    ) -> Dict[str, Any]:


        url = (
            f"{self.endpoints['hls_api']}/"
            f"{internal_video_id}"
            f"?club_token={award}"
        )


        response = self._request(
            "GET",
            url,
            "hls_options",
        )


        data = self._json(
            response,
            "hls_options",
        )


        video_balancer = data.get(
            "video_balancer"
        )


        if not isinstance(
            video_balancer,
            dict,
        ):


            raise RutubeScrapperError(
                "video_balancer was not found"
            )


        m3u8_url = (
            video_balancer.get("m3u8")
            or video_balancer.get("default")
        )


        if not m3u8_url:


            raise RutubeScrapperError(
                "No video balancer URL found"
            )


        return {
            "raw":
                data,


            "video_balancer":
                video_balancer,


            "m3u8_url":
                m3u8_url,
        }


    # ========================================================
    # LIVE OPTIONS
    # ========================================================


    def get_live_options(
        self,
        video_id: str,
    ) -> Dict[str, Any]:


        """
        Получение JSON options для live-видео.


        В отличие от старого VOD flow здесь используем
        публичный play/options endpoint с format=json.


        no_404=true позволяет получить JSON-ответ вместо
        простого 404 в некоторых случаях.
        """


        url = (
            f"{self.endpoints['hls_api']}/"
            f"{video_id}/"
            f"?format=json"
            f"&no_404=true"
        )


        response = self._request(
            "GET",
            url,
            f"live_options_{video_id}",
        )


        return self._json(
            response,
            f"live_options_{video_id}",
        )


    def get_live_streams(
        self,
        video_id: str,
    ) -> List[str]:


        data = self.get_live_options(
            video_id
        )


        result: List[str] = []


        live_streams = data.get(
            "live_streams",
            {},
        )


        if not isinstance(
            live_streams,
            dict,
        ):
            return result


        hls = live_streams.get(
            "hls",
            [],
        )


        if not isinstance(
            hls,
            list,
        ):
            return result


        for item in hls:


            if isinstance(
                item,
                str,
            ):


                if item.strip():
                    result.append(
                        item.strip()
                    )


                continue


            if not isinstance(
                item,
                dict,
            ):
                continue


            stream_url = (
                item.get("url")
                or item.get("src")
                or item.get("stream")
            )


            if stream_url:


                result.append(
                    str(stream_url)
                )


        return result


    # ========================================================
    # MASTER M3U8
    # ========================================================


    def get_m3u8(
        self,
        url: str,
    ) -> str:


        response = self._request(
            "GET",
            url,
            "master_m3u8",
        )


        return response.text.strip()


    # ========================================================
    # M3U8 PARSER
    # ========================================================


    def parse_m3u8(
        self,
        text: str,
    ) -> List[StreamInfo]:


        lines = [
            line.strip()
            for line in text.splitlines()
            if line.strip()
        ]


        streams: List[StreamInfo] = []


        for index, line in enumerate(
            lines
        ):


            if not line.startswith(
                "#EXT-X-STREAM-INF:"
            ):
                continue


            attributes_text = line[
                len("#EXT-X-STREAM-INF:")
            ]


            attributes = (
                self._parse_m3u8_attributes(
                    attributes_text
                )
            )


            stream_url = None


            for next_line in lines[
                index + 1:
            ]:


                if next_line.startswith(
                    "#"
                ):
                    continue


                stream_url = next_line
                break


            if not stream_url:
                continue


            resolution = attributes.get(
                "RESOLUTION"
            )


            width = None
            height = None


            if resolution:


                match = re.match(
                    r"^(\d+)x(\d+)$",
                    resolution,
                )


                if match:


                    width = int(
                        match.group(1)
                    )


                    height = int(
                        match.group(2)
                    )


            bandwidth = self._to_int(
                attributes.get(
                    "BANDWIDTH"
                )
            )


            average_bandwidth = (
                self._to_int(
                    attributes.get(
                        "AVERAGE-BANDWIDTH"
                    )
                )
            )


            frame_rate = self._to_float(
                attributes.get(
                    "FRAME-RATE"
                )
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
                codecs=attributes.get(
                    "CODECS"
                ),
                mime_type=attributes.get(
                    "MIME-TYPE"
                ),
                frame_rate=frame_rate,
                audio=attributes.get(
                    "AUDIO"
                ),
                video=attributes.get(
                    "VIDEO"
                ),
                url=stream_url,
                url_hash=self._hash_url(
                    stream_url
                ),
                source_index=len(
                    streams
                ),
                raw_attributes=attributes,
            )


            stream.quality_score = (
                self.calculate_quality_score(
                    stream
                )
            )


            streams.append(
                stream
            )


        return streams


    # ========================================================
    # M3U8 ATTRIBUTE PARSER
    # ========================================================


    @staticmethod
    def _parse_m3u8_attributes(
        text: str,
    ) -> Dict[str, str]:


        result = {}


        pattern = re.compile(
            r"""
            ([A-Z0-9\-]+)
            =
            (?:
                "([^"]*)"
                |
                ([^,]*)
            )
            """,
            re.VERBOSE,
        )


        for match in pattern.finditer(
            text
        ):


            key = match.group(1)


            value = (
                match.group(2)
                if match.group(2)
                is not None
                else match.group(3)
            )


            result[key] = (
                value.strip()
            )


        return result


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
                timeout=self.timeout,
                verify=self.verify_ssl,
                stream=True,
            )


            elapsed = (
                time.perf_counter()
                - started
            ) * 1000


            stream.response_time_ms = round(
                elapsed,
                3,
            )


            stream.http_status = (
                response.status_code
            )


            stream.content_type = (
                response.headers.get(
                    "Content-Type"
                )
            )


            content_length = (
                response.headers.get(
                    "Content-Length"
                )
            )


            if content_length:


                stream.response_size = (
                    self._to_int(
                        content_length
                    )
                )


            stream.alive = (
                response.ok
            )


            response.close()


        except Exception:


            stream.alive = False


            stream.response_time_ms = round(
                (
                    time.perf_counter()
                    - started
                ) * 1000,
                3,
            )


        return stream


    # ========================================================
    # QUALITY
    # ========================================================


    @staticmethod
    def calculate_quality_score(
        stream: StreamInfo,
    ) -> float:


        score = 0.0


        if stream.width and stream.height:


            pixels = (
                stream.width
                * stream.height
            )


            if pixels >= 3840 * 2160:
                score += 100


            elif pixels >= 1920 * 1080:
                score += 80


            elif pixels >= 1280 * 720:
                score += 60


            elif pixels >= 854 * 480:
                score += 40


            elif pixels >= 640 * 360:
                score += 20


        if stream.bandwidth:


            score += min(
                stream.bandwidth / 1_000_000,
                50,
            )


        if stream.frame_rate:


            score += min(
                stream.frame_rate,
                30,
            )


        return round(
            score,
            3,
        )


    # ========================================================
    # HELPERS
    # ========================================================


    @staticmethod
    def _hash_url(
        url: str,
    ) -> str:


        return hashlib.sha256(
            url.encode(
                "utf-8",
                errors="ignore",
            )
        ).hexdigest()


    @staticmethod
    def _to_int(
        value: Any,
    ) -> Optional[int]:


        if value is None:
            return None


        try:
            return int(
                float(str(value).strip())
            )
        except Exception:
            return None


    @staticmethod
    def _to_float(
        value: Any,
    ) -> Optional[float]:


        if value is None:
            return None


        try:
            return float(
                str(value).strip()
            )
        except Exception:
            return None


    @staticmethod
    def _extract_author(
        data: Dict[str, Any],
    ) -> Optional[str]:


        author = data.get(
            "author"
        )


        if isinstance(
            author,
            dict,
        ):


            return (
                author.get("name")
                or author.get("title")
                or author.get("username")
            )


        if author is not None:
            return str(author)


        return None


    @staticmethod
    def _extract_author_id(
        data: Dict[str, Any],
    ) -> Optional[str]:


        author = data.get(
            "author"
        )


        if isinstance(
            author,
            dict,
        ):


            value = (
                author.get("id")
                or author.get("uuid")
            )


            if value is not None:
                return str(value)


        value = (
            data.get("author_id")
            or data.get("authorId")
        )


        if value is not None:
            return str(value)


        return None


    @staticmethod
    def _extract_category(
        data: Dict[str, Any],
    ) -> Optional[str]:


        category = data.get(
            "category"
        )


        if isinstance(
            category,
            dict,
        ):


            return (
                category.get("name")
                or category.get("title")
            )


        if category is not None:
            return str(category)


        return None


    @staticmethod
    def _sanitize_visitor(
        data: Dict[str, Any],
    ) -> Dict[str, Any]:


        result = dict(data)


        for key in (
            "club_params_encrypted",
            "token",
            "access_token",
            "refresh_token",
        ):


            if key in result:
                result[key] = "<REDACTED>"


        return result


    @staticmethod
    def _json(
        response: requests.Response,
        name: str,
    ) -> Dict[str, Any]:


        try:
            value = response.json()
        except Exception as exc:
            raise RutubeScrapperError(
                f"{name}: invalid JSON: {exc}"
            ) from exc


        if not isinstance(
            value,
            dict,
        ):


            raise RutubeScrapperError(
                f"{name}: JSON root is not an object"
            )


        return value


# ============================================================
# TV DISCOVERY DATA
# ============================================================


@dataclass
class TVCard:
    name: str = ""
    video_id: str = ""
    category: str = ""
    url: str = ""
    source: str = ""
    shelf: str = ""
    marker: str = ""
    raw: Dict[str, Any] = field(
        default_factory=dict
    )


# ============================================================
# DISCOVERY HELPERS
# ============================================================


TECHNICAL_VIDEO_ID_TOKENS = {
    "tvfavorites",
    "history",
    "topic",
    "assets",
    "autowidget",
    "feedsource",
}


LIVE_PATH_MARKERS = (
    "/live/",
    "/tv/",
    "/online/",
    "/broadcast/",
    "/channel/",
)


VIDEO_ID_RE = re.compile(
    r"/video/([0-9a-fA-F-]{20,})"
)


JSON_VIDEO_ID_KEYS = (
    "video_id",
    "videoId",
    "video",
    "id",
    "uuid",
)


TV_NAME_KEYS = (
    "title",
    "name",
    "channel_name",
    "channelName",
)


CATEGORY_KEYS = (
    "category",
    "category_name",
    "categoryName",
)


SHELF_KEYS = (
    "shelf",
    "shelf_name",
    "shelfName",
    "section",
    "section_name",
)


# ============================================================
# TV DISCOVERY
# ============================================================


class TVDiscoveryMixin:


    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)


    @staticmethod
    def _clean_text(
        value: Any,
    ) -> str:


        if value is None:
            return ""


        text = html.unescape(
            str(value)
        )


        text = re.sub(
            r"\s+",
            " ",
            text,
        )


        return text.strip()


    @staticmethod
    def _looks_like_technical_video_id(
        value: Any,
    ) -> bool:


        if value is None:
            return True


        text = str(value).strip()


        if not text:
            return True


        low = text.lower()


        for token in TECHNICAL_VIDEO_ID_TOKENS:


            if token in low:
                return True


        if low.startswith(
            (
                "banner-",
                "feedsource-",
            )
        ):
            return True


        return False


    @classmethod
    def _extract_video_id_from_url(
        cls,
        url: str,
    ) -> Optional[str]:


        if not url:
            return None


        match = VIDEO_ID_RE.search(
            url
        )


        if match:


            value = match.group(1)


            if not cls._looks_like_technical_video_id(
                value
            ):


                return value


        parsed = urlparse(
            url
        )


        path_parts = [
            part
            for part in parsed.path.split("/")
            if part
        ]


        for index, part in enumerate(
            path_parts
        ):


            if part.lower() == "video":
                if index + 1 < len(
                    path_parts
                ):


                    candidate = (
                        path_parts[index + 1]
                    )


                    if not cls._looks_like_technical_video_id(
                        candidate
                    ):
                        return candidate


        return None


    @classmethod
    def _extract_video_id(
        cls,
        value: Any,
    ) -> Optional[str]:


        if isinstance(
            value,
            str,
        ):


            text = value.strip()


            if not text:
                return None


            direct = cls._extract_video_id_from_url(
                text
            )


            if direct:
                return direct


            if (
                not cls._looks_like_technical_video_id(
                    text
                )
                and re.fullmatch(
                    r"[0-9a-fA-F-]{20,}",
                    text,
                )
            ):
                return text


            return None


        if not isinstance(
            value,
            dict,
        ):
            return None


        for key in JSON_VIDEO_ID_KEYS:


            candidate = value.get(
                key
            )


            if isinstance(
                candidate,
                dict,
            ):


                nested = cls._extract_video_id(
                    candidate
                )


                if nested:
                    return nested


            else:


                nested = cls._extract_video_id(
                    candidate
                )


                if nested:
                    return nested


        for key in (
            "url",
            "link",
            "href",
            "canonical_url",
        ):


            candidate = value.get(
                key
            )


            if isinstance(
                candidate,
                str,
            ):


                nested = (
                    cls._extract_video_id_from_url(
                        candidate
                    )
                )


                if nested:
                    return nested


        return None


    @classmethod
    def _extract_name(
        cls,
        value: Dict[str, Any],
    ) -> str:


        for key in TV_NAME_KEYS:


            candidate = value.get(
                key
            )


            if candidate is None:
                continue


            if isinstance(
                candidate,
                dict,
            ):


                candidate = (
                    candidate.get("title")
                    or candidate.get("name")
                )


            text = cls._clean_text(
                candidate
            )


            if text:
                return text


        return ""


    @classmethod
    def _extract_category(
        cls,
        value: Dict[str, Any],
    ) -> str:


        for key in CATEGORY_KEYS:


            candidate = value.get(
                key
            )


            if isinstance(
                candidate,
                dict,
            ):


                candidate = (
                    candidate.get("name")
                    or candidate.get("title")
                )


            text = cls._clean_text(
                candidate
            )


            if text:
                return text


        return ""


    @classmethod
    def _extract_shelf(
        cls,
        value: Dict[str, Any],
    ) -> str:


        for key in SHELF_KEYS:


            candidate = value.get(
                key
            )


            if isinstance(
                candidate,
                dict,
            ):


                candidate = (
                    candidate.get("name")
                    or candidate.get("title")
                )


            text = cls._clean_text(
                candidate
            )


            if text:
                return text


        return ""


    @classmethod
    def _is_live_card(
        cls,
        value: Dict[str, Any],
    ) -> bool:


        if not isinstance(
            value,
            dict,
        ):
            return False


        text_parts = []


        for key in (
            "title",
            "name",
            "description",
            "type",
            "content_type",
            "kind",
            "url",
        ):


            candidate = value.get(
                key
            )


            if candidate is not None:
                text_parts.append(
                    str(candidate)
                )


        text = " ".join(
            text_parts
        ).lower()


        live_tokens = (
            "live",
            "эфир",
            "телеканал",
            "телеканалы",
            "tv",
            "channel",
            "broadcast",
            "online",
        )


        return any(
            token in text
            for token in live_tokens
        )


    @classmethod
    def _card_from_object(
        cls,
        value: Dict[str, Any],
        category: str = "",
        shelf: str = "",
        source: str = "",
    ) -> Optional[TVCard]:


        if not isinstance(
            value,
            dict,
        ):
            return None


        video_id = cls._extract_video_id(
            value
        )


        if not video_id:
            return None


        name = cls._extract_name(
            value
        )


        if not name:
            name = video_id


        card_category = (
            cls._extract_category(
                value
            )
            or category
        )


        card_shelf = (
            cls._extract_shelf(
                value
            )
            or shelf
        )


        url = ""


        for key in (
            "url",
            "link",
            "href",
            "canonical_url",
        ):


            candidate = value.get(
                key
            )


            if candidate:
                url = str(
                    candidate
                )
                break


        if not cls._is_live_card(
            value
        ):


            if not any(
                marker in url.lower()
                for marker in LIVE_PATH_MARKERS
            ):


                return None


        return TVCard(
            name=name,
            video_id=video_id,
            category=card_category,
            url=url,
            source=source,
            shelf=card_shelf,
            marker="",
            raw=value,
        )


    @classmethod
    def _walk_json_for_cards(
        cls,
        value: Any,
        category: str = "",
        shelf: str = "",
        source: str = "",
    ) -> List[TVCard]:


        result: List[TVCard] = []


        if isinstance(
            value,
            dict,
        ):


            current_category = (
                cls._extract_category(
                    value
                )
                or category
            )


            current_shelf = (
                cls._extract_shelf(
                    value
                )
                or shelf
            )


            card = cls._card_from_object(
                value,
                category=current_category,
                shelf=current_shelf,
                source=source,
            )


            if card:
                result.append(
                    card
                )


            for child in value.values():


                result.extend(
                    cls._walk_json_for_cards(
                        child,
                        category=current_category,
                        shelf=current_shelf,
                        source=source,
                    )
                )


        elif isinstance(
            value,
            list,
        ):


            for child in value:


                result.extend(
                    cls._walk_json_for_cards(
                        child,
                        category=category,
                        shelf=shelf,
                        source=source,
                    )
                )


        return result


    def fetch_discovery_source(
        self,
        url: str,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:


        response = self._request(
            "GET",
            url,
            "tv_discovery",
        )


        text = response.text


        data = None


        try:
            parsed = response.json()


            if isinstance(
                parsed,
                dict,
            ):
                data = parsed


        except Exception:
            data = None


        return text, data


    def discover_from_url(
        self,
        url: str,
        category: str = "",
        shelf: str = "",
        source: str = "",
    ) -> List[TVCard]:


        result: List[TVCard] = []


        try:


            text, data = (
                self.fetch_discovery_source(
                    url
                )
            )


            if data is not None:


                result.extend(
                    self._walk_json_for_cards(
                        data,
                        category=category,
                        shelf=shelf,
                        source=source,
                    )
                )


            result.extend(
                self._parse_tv_page(
                    text,
                    category=category,
                    shelf=shelf,
                    source=source,
                )
            )


        except Exception as exc:


            logging.warning(
                "TV discovery failed: %s: %s",
                url,
                exc,
            )


        return result


    # ========================================================
    # HTML TV PARSER
    # ========================================================


    @classmethod
    def _parse_tv_page(
        cls,
        text: str,
        category: str = "",
        shelf: str = "",
        source: str = "",
    ) -> List[TVCard]:


        result: List[TVCard] = []


        if not text:
            return result


        decoded = html.unescape(
            text
        )


        # ----------------------------------------------------
        # Direct Rutube video URLs.
        # ----------------------------------------------------


        seen_positions = set()


        for match in re.finditer(
            r'https?://rutube\.ru/video/[0-9a-fA-F-]{20,}/?',
            decoded,
            re.IGNORECASE,
        ):


            url = match.group(0)


            if match.start() in seen_positions:
                continue


            seen_positions.add(
                match.start()
            )


            video_id = (
                cls._extract_video_id_from_url(
                    url
                )
            )


            if not video_id:
                continue


            start = max(
                0,
                match.start() - 700,
            )


            end = min(
                len(decoded),
                match.end() + 700,
            )


            context = decoded[
                start:end
            ]


            name = ""


            title_match = re.search(
                r'"(?:title|name)"\s*:\s*"([^"]+)"',
                context,
                re.IGNORECASE,
            )


            if title_match:


                name = cls._clean_text(
                    title_match.group(1)
                )


            if not name:


                heading_match = re.search(
                    r"<(?:h[1-6]|title)[^>]*>"
                    r"\s*(.*?)\s*"
                    r"</(?:h[1-6]|title)>",
                    context,
                    re.IGNORECASE
                    | re.DOTALL,
                )


                if heading_match:


                    name = cls._clean_text(
                        re.sub(
                            r"<[^>]+>",
                            " ",
                            heading_match.group(1),
                        )
                    )


            if not name:
                name = video_id


            result.append(
                TVCard(
                    name=name,
                    video_id=video_id,
                    category=category,
                    url=url,
                    source=source,
                    shelf=shelf,
                    marker="",
                    raw={
                        "url":
                            url,
                        "context":
                            context,
                    },
                )
            )


        # ----------------------------------------------------
        # JSON embedded objects.
        # ----------------------------------------------------


        for match in re.finditer(
            r'\{[^{}]{0,10000}"(?:video_id|videoId)"'
            r'[^{}]{0,10000}\}',
            decoded,
            re.IGNORECASE,
        ):


            raw_object = match.group(0)


            try:


                obj = json.loads(
                    raw_object
                )


            except Exception:
                continue


            if not isinstance(
                obj,
                dict,
            ):
                continue


            card = cls._card_from_object(
                obj,
                category=category,
                shelf=shelf,
                source=source,
            )


            if card:
                result.append(
                    card
                )


        return result


    # ========================================================
    # CATEGORY DISCOVERY
    # ========================================================


    @staticmethod
    def _extract_category_urls(
        text: str,
    ) -> List[Tuple[str, str]]:


        result = []


        if not text:
            return result


        decoded = html.unescape(
            text
        )


        pattern = re.compile(
            r'href\s*=\s*["\']'
            r'([^"\']*'
            r'/feeds/live/tvprogramm/'
            r'[^"\']*)'
            r'["\']',
            re.IGNORECASE,
        )


        seen = set()


        for match in pattern.finditer(
            decoded
        ):


            raw_url = match.group(1)


            url = urljoin(
                DISCOVERY_SOURCE_URL,
                raw_url,
            )


            parsed = urlparse(
                url
            )


            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                "",
                "",
                "",
            ))


            if normalized in seen:
                continue


            seen.add(
                normalized
            )


            parts = [
                part
                for part in parsed.path.split("/")
                if part
            ]


            category = (
                parts[-1]
                if parts
                else "category"
            )


            category = category.replace(
                "-",
                " ",
            ).strip()


            result.append(
                (
                    category,
                    normalized,
                )
            )


        return result


    def discover_categories(
        self,
    ) -> List[Tuple[str, str]]:


        categories = []


        try:


            text, _ = (
                self.fetch_discovery_source(
                    DISCOVERY_SOURCE_URL
                )
            )


            categories.extend(
                self._extract_category_urls(
                    text
                )
            )


        except Exception as exc:


            logging.warning(
                "Category discovery failed: %s",
                exc,
            )


        existing = {
            url
            for _, url in categories
        }


        for name, url in (
            FALLBACK_CATEGORY_URLS.items()
        ):


            if url not in existing:


                categories.append(
                    (
                        name,
                        url,
                    )
                )


        return categories


    # ========================================================
    # PAGINATION
    # ========================================================


    @staticmethod
    def _page_url(
        base_url: str,
        page: int,
    ) -> str:


        parsed = urlparse(
            base_url
        )


        query = parse_qs(
            parsed.query,
            keep_blank_values=True,
        )


        query["page"] = [
            str(page)
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


    def scrape_category(
        self,
        category: str,
        url: str,
        max_pages: int = DEFAULT_MAX_PAGES,
        max_empty_pages: int = DEFAULT_MAX_EMPTY_PAGES,
    ) -> List[TVCard]:


        all_cards: List[TVCard] = []


        empty_pages = 0


        for page in range(
            1,
            max_pages + 1,
        ):


            page_url = (
                self._page_url(
                    url,
                    page,
                )
            )


            try:


                cards = self.discover_from_url(
                    page_url,
                    category=category,
                    shelf=category,
                    source="rutube_category",
                )


            except Exception as exc:


                logging.warning(
                    "Category page failed "
                    "category=%s page=%s: %s",
                    category,
                    page,
                    exc,
                )


                cards = []


            if not cards:


                empty_pages += 1


                if (
                    empty_pages
                    >= max_empty_pages
                ):
                    break


            else:


                empty_pages = 0


                all_cards.extend(
                    cards
                )


        return all_cards


    # ========================================================
    # FULL CATALOG DISCOVERY
    # ========================================================


    def scrape_tv_catalog(
        self,
        max_pages: int = DEFAULT_MAX_PAGES,
        max_empty_pages: int = DEFAULT_MAX_EMPTY_PAGES,
    ) -> List[TVCard]:


        categories = (
            self.discover_categories()
        )


        logging.info(
            "TV categories discovered: %d",
            len(categories),
        )


        all_cards: List[TVCard] = []


        for category, url in categories:


            logging.info(
                "TV CATEGORY: %s -> %s",
                category,
                url,
            )


            cards = self.scrape_category(
                category=category,
                url=url,
                max_pages=max_pages,
                max_empty_pages=max_empty_pages,
            )


            all_cards.extend(
                cards
            )


            logging.info(
                "TV CATEGORY RESULT: "
                "%s cards=%d",
                category,
                len(cards),
            )


        # Additional official TV programme feed.
        try:


            program_cards = (
                self.discover_from_url(
                    TV_PROGRAM_SOURCE_URL,
                    category="TV Programme",
                    shelf="TV Programme",
                    source="rutube_tv_program",
                )
            )


            all_cards.extend(
                program_cards
            )


            logging.info(
                "TV PROGRAM RESULT: cards=%d",
                len(program_cards),
            )


        except Exception as exc:


            logging.warning(
                "TV programme discovery failed: %s",
                exc,
            )


        return all_cards


# ============================================================
# MAIN SCRAPER COMPOSITION
# ============================================================


class RutubeFullScrapper(
    TVDiscoveryMixin,
    RutubeScrapper,
):


    pass


# ============================================================
# OUTPUT HELPERS
# ============================================================


def utc_now_iso() -> str:


    return (
        datetime.now(
            timezone.utc
        )
        .isoformat()
    )


def ensure_output_dir() -> Path:


    path = Path(
        OUTPUT_DIR
    )


    path.mkdir(
        parents=True,
        exist_ok=True,
    )


    return path


def write_json(
    path: Path,
    value: Any,
) -> None:


    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def write_jsonl(
    path: Path,
    rows: List[Dict[str, Any]],
) -> None:


    with path.open(
        "w",
        encoding="utf-8",
    ) as fh:


        for row in rows:


            fh.write(
                json.dumps(
                    row,
                    ensure_ascii=False,
                )
                + "\n"
            )


def write_text(
    path: Path,
    text: str,
) -> None:


    path.write_text(
        text,
        encoding="utf-8",
    )


# ============================================================
# M3U
# ============================================================


def make_m3u(
    rows: List[Dict[str, Any]],
) -> str:


    lines = [
        "#EXTM3U",
    ]


    for row in rows:


        name = str(
            row.get(
                "name",
                "",
            )
        )


        group = str(
            row.get(
                "group",
                RUTUBE_GROUP,
            )
        )


        epg_id = str(
            row.get(
                "tvg_id",
                "",
            )
        )


        logo = str(
            row.get(
                "logo",
                "",
            )
        )


        url = str(
            row.get(
                "url",
                "",
            )
        )


        attributes = [
            f'tvg-id="{epg_id}"'
            if epg_id
            else "",
            f'tvg-name="{name}"',
            f'group-title="{group}"',
            f'tvg-logo="{logo}"'
            if logo
            else "",
        ]


        attributes = [
            item
            for item in attributes
            if item
        ]


        lines.append(
            "#EXTINF:-1 "
            + " ".join(
                attributes
            )
            + ","
            + name
        )


        lines.append(
            "#EXTVLCOPT:http-referrer=https://rutube.ru/"
        )


        lines.append(
            "#EXTVLCOPT:http-user-agent="
            + M3U_USER_AGENT
        )


        lines.append(
            url
        )


    return "\n".join(
        lines
    ) + "\n"


# ============================================================
# STREAM ROW BUILDER
# ============================================================


def stream_to_dict(
    stream: StreamInfo,
) -> Dict[str, Any]:


    return asdict(
        stream
    )


def card_to_dict(
    card: TVCard,
) -> Dict[str, Any]:


    return asdict(
        card
    )


# ============================================================
# SINGLE VIDEO FLOW
# ============================================================


def process_single_video(
    scraper: RutubeFullScrapper,
    video_id: str,
    test_streams: bool = True,
) -> Dict[str, Any]:


    started = time.perf_counter()


    video = scraper.video(
        video_id
    )


    award_data = None
    balancer = None
    master_streams = []


    try:


        award_data = scraper.award(
            video.internal_id
        )


        balancer = scraper.get_balancer(
            video.internal_id,
            award_data["award"],
        )


        master_url = (
            balancer["m3u8_url"]
        )


        master_text = scraper.get_m3u8(
            master_url
        )


        master_streams = (
            scraper.parse_m3u8(
                master_text
            )
        )


        if test_streams:


            tested = []


            for stream in master_streams:


                tested.append(
                    scraper.test_stream(
                        stream
                    )
                )


            master_streams = tested


    except Exception as exc:


        logging.warning(
            "Single video HLS flow failed: %s",
            exc,
        )


    elapsed = (
        time.perf_counter()
        - started
    ) * 1000


    return {
        "video":
            asdict(video),


        "award":
            award_data,


        "balancer":
            balancer,


        "streams":
            [
                stream_to_dict(
                    stream
                )
                for stream
                in master_streams
            ],


        "duration_ms":
            round(
                elapsed,
                3,
            ),
    }


# ============================================================
# TV CARD SERIALIZATION
# ============================================================


def tv_card_rows(
    cards: List[TVCard],
) -> List[Dict[str, Any]]:


    rows = []


    for index, card in enumerate(
        cards,
        start=1,
    ):


        rows.append({
            "index":
                index,


            "name":
                card.name,


            "video_id":
                card.video_id,


            "category":
                card.category,


            "shelf":
                card.shelf,


            "url":
                card.url,


            "source":
                card.source,


            "marker":
                card.marker,
        })


    return rows


# ============================================================
# TV STREAM RESOLUTION
# ============================================================


def resolve_tv_streams(
    scraper: RutubeFullScrapper,
    cards: List[TVCard],
) -> List[Dict[str, Any]]:


    result = []


    total = len(
        cards
    )


    for index, card in enumerate(
        cards,
        start=1,
    ):


        logging.info(
            "TV HLS %d/%d: %s [%s]",
            index,
            total,
            card.name,
            card.video_id,
        )


        try:


            streams = (
                scraper.get_live_streams(
                    card.video_id
                )
            )


        except Exception as exc:


            logging.warning(
                "TV HLS failed "
                "name=%s video_id=%s: %s",
                card.name,
                card.video_id,
                exc,
            )


            streams = []


        for stream_index, url in enumerate(
            streams
        ):


            result.append({
                "name":
                    card.name,


                "video_id":
                    card.video_id,


                "category":
                    card.category,


                "shelf":
                    card.shelf,


                "source":
                    card.source,


                "stream_index":
                    stream_index,


                "url":
                    url,


                "group":
                    RUTUBE_GROUP,


                "tvg_id":
                    "",


                "logo":
                    "",
            })


    return result


# ============================================================
# REPORT
# ============================================================


def build_report(
    scraper: RutubeFullScrapper,
    cards: List[TVCard],
    streams: List[Dict[str, Any]],
    started_at: str,
    finished_at: str,
) -> Dict[str, Any]:


    return {
        "version":
            VERSION,


        "started_at":
            started_at,


        "finished_at":
            finished_at,


        "tv_cards":
            len(cards),


        "streams":
            len(streams),


        "requests":
            len(
                scraper.request_stats
            ),


        "request_stats":
            [
                asdict(
                    stat
                )
                for stat
                in scraper.request_stats
            ],


        "deduplication":
            False,


        "deduplication_method":
            "none",


        "stream_source":
            "live_streams.hls",
    }


# ============================================================
# LOGGING
# ============================================================


def configure_logging() -> None:


    output_dir = ensure_output_dir()


    log_path = (
        output_dir
        / "rutube_tv.log"
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
# ENVIRONMENT
# ============================================================


def env_int(
    name: str,
    default: int,
) -> int:


    value = os.getenv(
        name
    )


    if value is None:
        return default


    try:
        return int(
            value
        )
    except Exception:
        return default


def env_bool(
    name: str,
    default: bool,
) -> bool:


    value = os.getenv(
        name
    )


    if value is None:
        return default


    value = value.strip().lower()


    if value in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return True


    if value in (
        "0",
        "false",
        "no",
        "off",
    ):
        return False


    return default


# ============================================================
# MAIN
# ============================================================


def main() -> int:


    configure_logging()


    parser = argparse.ArgumentParser(
        description=(
            "Rutube Full Scraper / "
            "Autonomous TV Discovery"
        )
    )


    parser.add_argument(
        "video_id",
        nargs="?",
        default=None,
    )


    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
    )


    parser.add_argument(
        "--no-test",
        action="store_true",
    )


    parser.add_argument(
        "--max-pages",
        type=int,
        default=env_int(
            "RUTUBE_MAX_PAGES",
            DEFAULT_MAX_PAGES,
        ),
    )


    parser.add_argument(
        "--max-empty-pages",
        type=int,
        default=env_int(
            "RUTUBE_MAX_EMPTY_PAGES",
            DEFAULT_MAX_EMPTY_PAGES,
        ),
    )


    args = parser.parse_args()


    scraper = RutubeFullScrapper(
        timeout=(
            10,
            args.timeout,
        )
    )


    output_dir = ensure_output_dir()


    started_at = utc_now_iso()


    scraper.started_at = (
        started_at
    )


    # --------------------------------------------------------
    # SINGLE VIDEO MODE
    # --------------------------------------------------------


    if args.video_id:


        logging.info(
            "MODE: SINGLE VIDEO"
        )


        try:


            result = process_single_video(
                scraper,
                args.video_id,
                test_streams=(
                    not args.no_test
                ),
            )


            write_json(
                output_dir
                / "single_video.json",
                result,
            )


            logging.info(
                "Single video completed"
            )


            return 0


        except Exception as exc:


            logging.exception(
                "Single video failed: %s",
                exc,
            )


            return 1


    # --------------------------------------------------------
    # AUTONOMOUS TV MODE
    # --------------------------------------------------------


    logging.info(
        "MODE: AUTONOMOUS TV DISCOVERY"
    )


    logging.info(
        "DISCOVERY SOURCE: %s",
        DISCOVERY_SOURCE_URL,
    )


    try:


        cards = (
            scraper.scrape_tv_catalog(
                max_pages=args.max_pages,
                max_empty_pages=(
                    args.max_empty_pages
                ),
            )
        )


        logging.info(
            "DISCOVERY COMPLETE: cards=%d",
            len(cards),
        )


        card_rows = tv_card_rows(
            cards
        )


        write_json(
            output_dir
            / JSON_FILENAME,
            card_rows,
        )


        write_jsonl(
            output_dir
            / JSONL_FILENAME,
            card_rows,
        )


        txt_lines = []


        for row in card_rows:


            txt_lines.append(
                (
                    f"{row['name']}\t"
                    f"{row['video_id']}\t"
                    f"{row['category']}\t"
                    f"{row['url']}"
                )
            )


        write_text(
            output_dir
            / TXT_FILENAME,
            "\n".join(
                txt_lines
            ) + (
                "\n"
                if txt_lines
                else ""
            ),
        )


        # ----------------------------------------------------
        # SECOND STAGE:
        # VIDEO ID -> HLS
        # ----------------------------------------------------


        logging.info(
            "HLS STAGE START: cards=%d",
            len(cards),
        )


        streams = resolve_tv_streams(
            scraper,
            cards,
        )


        logging.info(
            "HLS STAGE COMPLETE: streams=%d",
            len(streams),
        )


        write_json(
            output_dir
            / "rutube_tv_streams.json",
            streams,
        )


        write_jsonl(
            output_dir
            / "rutube_tv_streams.jsonl",
            streams,
        )


        m3u = make_m3u(
            streams
        )


        write_text(
            output_dir
            / M3U_FILENAME,
            m3u,
        )


        finished_at = utc_now_iso()


        scraper.finished_at = (
            finished_at
        )


        report = build_report(
            scraper,
            cards,
            streams,
            started_at,
            finished_at,
        )


        write_json(
            output_dir
            / "rutube_tv_report.json",
            report,
        )


        logging.info(
            "AUTONOMOUS TV DISCOVERY FINISHED"
        )


        logging.info(
            "TV CARDS: %d",
            len(cards),
        )


        logging.info(
            "HLS STREAMS: %d",
            len(streams),
        )


        return 0


    except KeyboardInterrupt:


        logging.warning(
            "Interrupted by user"
        )


        return 130


    except Exception as exc:


        logging.exception(
            "Autonomous mode failed: %s",
            exc,
        )


        return 1


# ============================================================
# ENTRY POINT
# ============================================================


if __name__ == "__main__":


    raise SystemExit(
        main()
    )