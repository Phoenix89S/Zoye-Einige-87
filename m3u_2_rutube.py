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


    # ========================================================
    # LOGIN
    # ========================================================


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


        stream.quality_score = (
            self.calculate_quality_score(
                stream
            )
        )


        return stream


    # ========================================================
    # QUALITY SCORE
    # ========================================================


    @staticmethod
    def calculate_quality_score(
        stream: StreamInfo,
    ) -> float:


        score = 0.0


        if (
            stream.width
            and stream.height
        ):


            pixels = (
                stream.width
                * stream.height
            )


            if pixels >= 3840 * 2160:
                score += 60


            elif pixels >= 2560 * 1440:
                score += 50


            elif pixels >= 1920 * 1080:
                score += 40


            elif pixels >= 1280 * 720:
                score += 30


            elif pixels >= 854 * 480:
                score += 20


            else:
                score += 10


        if stream.bandwidth:


            score += min(
                25,
                stream.bandwidth / 400_000,
            )


        if stream.frame_rate:


            if stream.frame_rate >= 50:
                score += 10


            elif stream.frame_rate >= 25:
                score += 7


            elif stream.frame_rate >= 20:
                score += 4


        if stream.alive is True:
            score += 15


        elif stream.alive is False:
            score -= 20


        return round(
            max(
                0.0,
                min(
                    score,
                    100.0
                ),
            ),
            3,
        )


    # ========================================================
    # LEGACY FULL SCRAPE
    # ========================================================


    def scrape(
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


            (
                f'#PLAYLIST:"'
                f'Rutube {video.internal_id}"'
            ),
        ]


        ordered = sorted(
            streams,
            key=lambda s: (
                s.height or 0,
                s.bandwidth or 0,
            ),
            reverse=True,
        )


        for index, stream in enumerate(
            ordered,
            start=1,
        ):


            if (
                only_alive
                and stream.alive is not True
            ):
                continue


            resolution = (
                stream.resolution
                or "unknown"
            )


            title = (
                video.title
                or f"Rutube {video.internal_id}"
            )


            if stream.bandwidth_kbps:


                title = (
                    f"{title} | "
                    f"{resolution} | "
                    f"{stream.bandwidth_kbps:.0f} kbps"
                )


            else:


                title = (
                    f"{title} | "
                    f"{resolution}"
                )


            output.append(
                (
                    f'#EXTINF:-1 '
                    f'tvg-id="rutube_'
                    f'{video.internal_id}_'
                    f'{index}" '
                    f'tvg-name="{title}",'
                    f'{title}'
                )
            )


            output.append(
                stream.url
            )


        return (
            "\n".join(output)
            + "\n"
        )


    # ========================================================
    # TV DISCOVERY
    # ========================================================


    def discover_tv_catalog(
        self,
        source_url: str = DISCOVERY_SOURCE_URL,
        max_pages: int = DEFAULT_MAX_PAGES,
        max_empty_pages: int = DEFAULT_MAX_EMPTY_PAGES,
    ) -> List[Dict[str, Any]]:


        """
        Основной автономный discovery.


        ПОСЛЕДОВАТЕЛЬНОСТЬ:
            1. Discovery полок (API + HTML)
            2. Обход каждой полки/категории
            3. Извлечение только реальных TV/live-карточек
            4. video_id каждой карточки
            5. (далее в scrape_tv_catalog) get_live_streams


        НИКАКОЙ ДЕДУПЛИКАЦИИ.


        Каждый найденный occurrence превращается
        в отдельную запись.


        Даже если:


            Первый канал / video123
            Первый канал / video123


        встречается дважды — обе записи остаются.


        Даже если:


            category=A
            category=B


        — обе записи остаются.


        Технические ID (tvfavorites, history, topic,
        assets, banner-*, autowidget и т.п.)
        не считаются карточками каналов.
        """



        category_pages = (
            self._discover_category_pages(
                source_url
            )
        )


        pages: List[
            Tuple[str, str]
        ] = []


        # Главная.
        pages.append(
            (
                "Все эфиры",
                source_url,
            )
        )


        # TV program.
        if not any(
            url == TV_PROGRAM_SOURCE_URL
            for _, url in pages
        ):


            pages.append(
                (
                    "Телеканалы",
                    TV_PROGRAM_SOURCE_URL,
                )
            )


        # Обнаруженные категории.
        for category_name, url in (
            category_pages
        ):


            if not any(
                existing_url == url
                for _, existing_url
                in pages
            ):


                pages.append(
                    (
                        category_name,
                        url,
                    )
                )


        # Fallback categories.
        for category_name, url in (
            FALLBACK_CATEGORY_URLS.items()
        ):


            if not any(
                existing_url == url
                for _, existing_url
                in pages
            ):


                pages.append(
                    (
                        category_name,
                        url,
                    )
                )


        records: List[
            Dict[str, Any]
        ] = []


        for category_name, category_url in pages:


            logging.info(
                "TV DISCOVERY CATEGORY: %s",
                category_name,
            )


            logging.info(
                "TV DISCOVERY URL: %s",
                category_url,
            )


            empty_pages = 0


            for page in range(
                1,
                max_pages + 1,
            ):


                page_url = (
                    self._make_page_url(
                        category_url,
                        page,
                    )
                )


                try:


                    response = self._request(
                        "GET",
                        page_url,
                        (
                            "tv_discovery_"
                            f"{self._safe_filename_part(category_name)}_"
                            f"{page}"
                        ),
                    )


                    html_text = (
                        response.text
                    )


                except Exception as exc:


                    logging.warning(
                        "Discovery page failed: %s | %s",
                        page_url,
                        exc,
                    )


                    break


                found = (
                    self._parse_tv_page(
                        html_text,
                        category_name,
                        page_url,
                    )
                )


                if not found:


                    empty_pages += 1


                    if (
                        empty_pages
                        >= max_empty_pages
                    ):


                        break


                    continue


                empty_pages = 0


                # ==================================================
                # КРИТИЧЕСКИ ВАЖНО:
                #
                # НИКАКОГО:
                #
                # set()
                # dict keyed by video_id
                # unique()
                # deduplicate()
                #
                # Просто extend().
                # ==================================================


                records.extend(
                    found
                )


                logging.info(
                    (
                        "TV DISCOVERY: "
                        "%s page=%d "
                        "found=%d total=%d"
                    ),
                    category_name,
                    page,
                    len(found),
                    len(records),
                )


                # Если HTML явно содержит ссылку
                # на следующую страницу — продолжаем.
                #
                # Если её нет, следующая страница всё равно
                # проверяется, потому что некоторые страницы
                # RUTUBE строятся динамически.
                if not self._has_next_page(
                    html_text,
                    page,
                ):


                    # Однако страницу 2 нужно проверить
                    # хотя бы один раз, если на первой были
                    # записи.
                    if page >= 2:
                        break


        logging.info(
            "TV DISCOVERY RAW TOTAL: %d",
            len(records),
        )


        return records


    # ========================================================
    # CATEGORY DISCOVERY
    # ========================================================


    def _discover_category_pages(
        self,
        source_url: str,
    ) -> List[
        Tuple[str, str]
    ]:


        result: List[
            Tuple[str, str]
        ] = []


        # ----------------------------------------------------
        # ЭТАП 1: API Discovery полок (shelves / resources)
        # из https://rutube.ru/api/feeds/live/
        #
        # Это основной источник полок.
        # Технические resources (tvfavorites, history, topic
        # и т.п.) отфильтровываются.
        # ----------------------------------------------------


        TECHNICAL_SHELF_KEYWORDS = {
            "tvfavorites",
            "history",
            "topic",
            "assets",
            "banner",
            "autowidget",
            "continue_watch",
            "popular_in_live",
            "live_by_topic",
            "tvarchive",
            "feedsource",
        }


        try:
            api_url = "https://rutube.ru/api/feeds/live/"
            response = self._request(
                "GET",
                api_url,
                "tv_api_shelves_discovery",
            )
            api_data = self._json(response, "tv_api_shelves")


            for tab in api_data.get("tabs", []):
                tab_name = tab.get("name") or "Без названия"
                tab_slug = tab.get("slug") or ""

                # Ссылка самой вкладки, если есть
                tab_url = tab.get("url") or tab.get("link", {}).get("url")
                if isinstance(tab_url, dict):
                    tab_url = tab_url.get("url")

                if tab_url and isinstance(tab_url, str):
                    if not any(u == tab_url for _, u in result):
                        result.append((tab_name, tab_url))

                # Resources внутри вкладки = полки
                for res in tab.get("resources", []):
                    res_name = (
                        res.get("name")
                        or res.get("extra_params", {}).get("name")
                        or "Полка"
                    )
                    res_url = res.get("site_url") or res.get("url")
                    object_id = str(res.get("object_id") or "").lower()

                    # Фильтр технических полок
                    if any(kw in object_id for kw in TECHNICAL_SHELF_KEYWORDS):
                        continue
                    if any(kw in (res_url or "").lower() for kw in TECHNICAL_SHELF_KEYWORDS):
                        continue
                    if any(kw in res_name.lower() for kw in (
                        "избранное", "вы смотрели", "баннер"
                    )):
                        continue

                    if res_url and isinstance(res_url, str):
                        # Если это API-url, превращаем в человеческий site_url
                        if "api." in res_url or "/api/" in res_url:
                            # Пропускаем чистые API-эндпоинты без site_url
                            if not res.get("site_url"):
                                continue
                            res_url = res.get("site_url")

                        if res_url and not any(u == res_url for _, u in result):
                            result.append((res_name, res_url))

            logging.info(
                "API shelves discovered: %d",
                len(result),
            )

        except Exception as exc:
            logging.warning(
                "API shelves discovery failed: %s",
                exc,
            )


        # ----------------------------------------------------
        # ЭТАП 2: HTML fallback (как было)
        # ----------------------------------------------------


        try:


            response = self._request(
                "GET",
                source_url,
                "tv_category_discovery",
            )


            text = response.text


        except Exception as exc:


            logging.warning(
                "Category discovery failed: %s",
                exc,
            )


            return result


        # Сначала ищем ссылки, у которых в видимом тексте
        # есть название категории.
        category_names = [
            "Федеральные",
            "Региональные",
            "Новости",
            "Развлекательные",
            "Кино и сериалы",
            "Спорт",
            "Детские",
            "Мультфильмы",
            "Музыка",
            "Познавательные",
            "Телемагазин",
            "18+",
            "Религия",
        ]



        # Обычные <a href=...>TEXT</a>
        anchor_pattern = re.compile(
            r"<a\b[^>]*"
            r'href\s*=\s*["\']([^"\']+)["\']'
            r"[^>]*>"
            r"(.*?)"
            r"</a>",
            re.IGNORECASE
            | re.DOTALL,
        )


        for match in anchor_pattern.finditer(
            text
        ):


            href = (
                html.unescape(
                    match.group(1)
                )
            )


            anchor_text = re.sub(
                r"<[^>]+>",
                " ",
                match.group(2),
            )


            anchor_text = (
                html.unescape(
                    anchor_text
                )
            )


            anchor_text = re.sub(
                r"\s+",
                " ",
                anchor_text,
            ).strip()


            if not href:
                continue


            absolute = urljoin(
                source_url,
                href,
            )


            parsed = urlparse(
                absolute
            )


            if parsed.netloc.lower() != (
                "rutube.ru"
            ):
                continue


            for category_name in (
                category_names
            ):


                if (
                    anchor_text.lower()
                    == category_name.lower()
                ):


                    if not any(
                        existing_url == absolute
                        for _, existing_url
                        in result
                    ):


                        result.append(
                            (
                                category_name,
                                absolute,
                            )
                        )


                    break


        return result


    # ========================================================
    # TV PAGE PARSER
    # ========================================================


    def _parse_tv_page(
        self,
        page_html: str,
        category: str,
        page_url: str,
    ) -> List[Dict[str, Any]]:
        """
        Extract ONLY real Rutube TV live cards.

        IMPORTANT:
        - Do not scan arbitrary /video/<id> links.
        - Do not scan arbitrary JSON ``id`` fields.
        - A channel occurrence is represented by a real /live/video/<id> URL.
        - The same channel may occur on several shelves/pages; those
          occurrences are intentionally preserved.
        - Repeated references to the same card inside one HTML page are
          markup duplicates, not additional catalog occurrences, so the
          exact same live URL is emitted only once per page.
        """

        records: List[Dict[str, Any]] = []
        seen_on_this_page = set()

        live_pattern = re.compile(
            r"(?P<href>(?:https?://rutube\.ru)?/live/video/(?P<id>[A-Za-z0-9_-]{10,})/?)",
            re.IGNORECASE,
        )

        for match in live_pattern.finditer(page_html):
            video_id = match.group("id")
            if not video_id:
                continue

            # Normalize only the card URL for the local markup check.
            # We deliberately DO NOT use this as global deduplication.
            card_key = video_id.lower()
            if card_key in seen_on_this_page:
                continue
            seen_on_this_page.add(card_key)

            position = match.start()
            context_start = max(0, position - 3500)
            context_end = min(len(page_html), position + 3500)
            context = page_html[context_start:context_end]

            title = self._extract_title_from_context(context, video_id)
            logo = self._extract_logo_from_context(context)

            # Never let generic UI text become a channel name.
            bad_titles = {
                "rutube", "rutube tv", "прямой эфир",
                "live", "сейчас", "смотреть",
                "главная", "телеканалы",
            }
            if title.strip().lower() in bad_titles:
                title = f"Rutube {video_id}"

            records.append({
                "video_id": video_id,
                "title": title,
                "category": category,
                "source_url": page_url,
                "catalog_url": f"https://rutube.ru/live/video/{video_id}/",
                "logo": logo,
                "card_type": "live",
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            })

        return records

    # ========================================================
    # TITLE EXTRACTION
    # ========================================================


    @staticmethod
    def _extract_title_from_context(
        context: str,
        video_id: str,
    ) -> str:


        patterns = [
            r'"title"\s*:\s*"([^"]+)"',


            r'"name"\s*:\s*"([^"]+)"',


            r'aria-label\s*=\s*["\']'
            r'([^"\']+)'
            r'["\']',


            r'title\s*=\s*["\']'
            r'([^"\']+)'
            r'["\']',


            r'<h[1-6][^>]*>'
            r'(.*?)'
            r'</h[1-6]>',
        ]


        for pattern in patterns:


            match = re.search(
                pattern,
                context,
                re.IGNORECASE
                | re.DOTALL,
            )


            if not match:
                continue


            value = (
                match.group(1)
            )


            value = re.sub(
                r"<[^>]+>",
                " ",
                value,
            )


            value = html.unescape(
                value
            )


            value = re.sub(
                r"\s+",
                " ",
                value,
            ).strip()


            if not value:
                continue


            # Не использовать технические значения
            # как название.
            if value.lower() in {
                "прямой эфир",
                "live",
                "сейчас",
                "смотреть",
            }:
                continue


            if len(value) > 500:
                continue


            return value


        return (
            f"Rutube {video_id}"
        )


    # ========================================================
    # LOGO EXTRACTION
    # ========================================================


    @staticmethod
    def _extract_logo_from_context(
        context: str,
    ) -> Optional[str]:


        patterns = [
            r'"logo"\s*:\s*"([^"]+)"',


            r'"avatar"\s*:\s*"([^"]+)"',


            r'"image"\s*:\s*"([^"]+)"',


            r'<img[^>]+src\s*=\s*'
            r'["\']([^"\']+)["\']',
        ]


        for pattern in patterns:


            match = re.search(
                pattern,
                context,
                re.IGNORECASE
                | re.DOTALL,
            )


            if not match:
                continue


            value = (
                html.unescape(
                    match.group(1)
                )
                .strip()
            )


            if not value:
                continue


            if value.startswith(
                "//"
            ):
                value = (
                    "https:"
                    + value
                )


            if value.startswith(
                "/"
            ):
                value = urljoin(
                    DISCOVERY_SOURCE_URL,
                    value,
                )


            if value.startswith(
                "http://"
            ) or value.startswith(
                "https://"
            ):
                return value


        return None


    # ========================================================
    # PAGE URL
    # ========================================================


    @staticmethod
    def _make_page_url(
        base_url: str,
        page: int,
    ) -> str:


        if page <= 1:
            return base_url


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


        new_query = urlencode(
            query,
            doseq=True,
        )


        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        ))


    # ========================================================
    # NEXT PAGE DETECTION
    # ========================================================


    @staticmethod
    def _has_next_page(
        page_html: str,
        current_page: int,
    ) -> bool:


        next_patterns = [
            r'href\s*=\s*["\'][^"\']*'
            r'page[-=]'
            r'\d+[^"\']*["\']',


            r'"page"\s*:\s*'
            r'["\']?'
            r'\d+',


            r'"hasNext"\s*:\s*true',


            r'"has_next"\s*:\s*true',


            r'"next"\s*:\s*',
        ]


        for pattern in next_patterns:


            if re.search(
                pattern,
                page_html,
                re.IGNORECASE,
            ):
                return True


        return False


    # ========================================================
    # TV CATALOG SCRAPE
    # ========================================================


    def scrape_tv_catalog(
        self,
        source_url: str = DISCOVERY_SOURCE_URL,
        max_pages: int = DEFAULT_MAX_PAGES,
        max_empty_pages: int = DEFAULT_MAX_EMPTY_PAGES,
    ) -> Dict[str, Any]:


        self.started_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )


        discovered = (
            self.discover_tv_catalog(
                source_url=source_url,
                max_pages=max_pages,
                max_empty_pages=max_empty_pages,
            )
        )


        result: List[
            Dict[str, Any]
        ] = []


        total = len(
            discovered
        )


        logging.info(
            "TV DISCOVERY PROCESSING %d RECORDS",
            total,
        )


        for number, record in enumerate(
            discovered,
            start=1,
        ):


            video_id = (
                record.get(
                    "video_id"
                )
            )


            if not video_id:
                continue


            title = (
                record.get(
                    "title"
                )
                or
                f"Rutube {video_id}"
            )


            category = (
                record.get(
                    "category"
                )
                or
                "Без категории"
            )


            logging.info(
                "[%d/%d] %s | %s | %s",
                number,
                total,
                category,
                title,
                video_id,
            )


            # ------------------------------------------------
            # Копируем record целиком.
            # ------------------------------------------------


            item = dict(
                record
            )


            item[
                "hls_streams"
            ] = []


            item[
                "stream_count"
            ] = 0


            item[
                "status"
            ] = "pending"


            try:


                # --------------------------------------------
                # Получаем video metadata.
                # --------------------------------------------


                video = self.video(
                    video_id
                )


                item[
                    "internal_id"
                ] = video.internal_id


                item[
                    "video_title"
                ] = video.title


                item[
                    "author"
                ] = video.author


                item[
                    "author_id"
                ] = video.author_id


                item[
                    "video_category"
                ] = video.category


                item[
                    "is_livestream"
                ] = (
                    self._detect_is_livestream(
                        video.raw
                    )
                )


                # Если HTML дал только техническое имя,
                # используем название API.
                #
                # При этом название категории НЕ меняем.
                if (
                    not title
                    or title.startswith(
                        "Rutube "
                    )
                ):


                    if video.title:
                        item[
                            "title"
                        ] = video.title


                # --------------------------------------------
                # Получаем live HLS.
                # --------------------------------------------


                hls_streams = (
                    self.get_live_streams(
                        video_id
                    )
                )


                # Никакой dedupe.
                item[
                    "hls_streams"
                ] = list(
                    hls_streams
                )


                item[
                    "stream_count"
                ] = len(
                    hls_streams
                )


                if hls_streams:


                    item[
                        "status"
                    ] = "ok"


                else:


                    item[
                        "status"
                    ] = "no_hls"


            except Exception as exc:


                item[
                    "status"
                ] = "error"


                item[
                    "error"
                ] = str(
                    exc
                )


                logging.warning(
                    (
                        "CHANNEL ERROR: "
                        "%s | %s"
                    ),
                    video_id,
                    exc,
                )


            result.append(
                item
            )


        self.finished_at = (
            datetime.now(
                timezone.utc
            ).isoformat()
        )


        total_hls = sum(
            len(
                item.get(
                    "hls_streams",
                    [],
                )
            )
            for item in result
        )


        with_hls = sum(
            1
            for item in result
            if item.get(
                "hls_streams"
            )
        )


        without_hls = (
            len(result)
            - with_hls
        )


        errors = sum(
            1
            for item in result
            if item.get(
                "status"
            ) == "error"
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


                "started_at":
                    self.started_at,


                "finished_at":
                    self.finished_at,


                "source_url":
                    source_url,


                "tv_program_url":
                    TV_PROGRAM_SOURCE_URL,


                "python_version":
                    sys.version,


                "platform":
                    sys.platform,
            },


            "settings": {
                "deduplication":
                    False,


                "deduplication_method":
                    "NONE",


                "group":
                    RUTUBE_GROUP,


                "epg":
                    M3U_EPG_URL,


                "user_agent":
                    M3U_USER_AGENT,
            },


            "statistics": {
                "discovered_records":
                    len(discovered),


                "processed_records":
                    len(result),


                "with_hls":
                    with_hls,


                "without_hls":
                    without_hls,


                "errors":
                    errors,


                "total_hls_urls":
                    total_hls,


                "categories":
                    self._category_statistics(
                        result
                    ),
            },


            "channels":
                result,


            "http": {
                "requests": [
                    asdict(stat)
                    for stat
                    in self.request_stats
                ]
            },
        }


    # ========================================================
    # LIVE DETECTION
    # ========================================================


    @staticmethod
    def _detect_is_livestream(
        data: Dict[str, Any],
    ) -> Optional[bool]:


        for key in (
            "is_livestream",
            "is_live",
            "livestream",
        ):


            if key in data:


                value = data[
                    key
                ]


                if isinstance(
                    value,
                    bool,
                ):
                    return value


                if isinstance(
                    value,
                    str,
                ):


                    return (
                        value.lower()
                        in {
                            "true",
                            "1",
                            "yes",
                        }
                    )


        return None


    # ========================================================
    # CATEGORY STATISTICS
    # ========================================================


    @staticmethod
    def _category_statistics(
        records: List[
            Dict[str, Any]
        ],
    ) -> Dict[str, Any]:


        result = {}


        for record in records:


            category = (
                record.get(
                    "category"
                )
                or
                "Без категории"
            )


            if category not in result:


                result[
                    category
                ] = {
                    "records":
                        0,


                    "with_hls":
                        0,


                    "hls_urls":
                        0,
                }


            result[
                category
            ][
                "records"
            ] += 1


            streams = (
                record.get(
                    "hls_streams",
                    [],
                )
            )


            result[
                category
            ][
                "hls_urls"
            ] += len(
                streams
            )


            if streams:


                result[
                    category
                ][
                    "with_hls"
                ] += 1


        return result


    # ========================================================
    # TV M3U
    # ========================================================


    @classmethod
    def make_tv_m3u(
        cls,
        records: List[
            Dict[str, Any]
        ],
    ) -> str:


        lines = [
            (
                "#EXTM3U "
                f'url-tvg="{M3U_EPG_URL}"'
            )
        ]


        # ----------------------------------------------------
        # НИКАКОЙ сортировки по уникальности.
        #
        # Порядок discovery сохраняется.
        # ----------------------------------------------------


        for index, record in enumerate(
            records,
            start=1,
        ):


            title = (
                record.get(
                    "title"
                )
                or
                record.get(
                    "video_title"
                )
                or
                f"Rutube TV {index}"
            )


            category = (
                record.get(
                    "category"
                )
                or
                "Без категории"
            )


            video_id = (
                record.get(
                    "video_id"
                )
                or
                ""
            )


            logo = (
                record.get(
                    "logo"
                )
                or
                ""
            )


            hls_streams = (
                record.get(
                    "hls_streams",
                    [],
                )
            )


            # -----------------------------------------------
            # Один occurrence канала может содержать
            # несколько HLS URLs.
            #
            # Каждый URL сохраняем отдельной записью.
            # -----------------------------------------------


            for stream_index, stream_url in enumerate(
                hls_streams,
                start=1,
            ):


                safe_title = (
                    cls._m3u_escape(
                        title
                    )
                )


                safe_category = (
                    cls._m3u_escape(
                        category
                    )
                )


                group = (
                    RUTUBE_GROUP
                )


                hierarchical_group = (
                    f"{group}/{category}"
                )


                safe_group = (
                    cls._m3u_escape(
                        group
                    )
                )


                safe_hierarchical_group = (
                    cls._m3u_escape(
                        hierarchical_group
                    )
                )


                tvg_id = (
                    "rutube_"
                    f"{video_id}"
                )


                # Не добавляем stream_index в tvg-id,
                # чтобы EPG мог сопоставляться с каналом.
                #
                # Даже если одна запись имеет несколько
                # потоков — каждый URL всё равно сохраняется.


                extinf = (
                    "#EXTINF:-1 "
                    f'tvg-id="{tvg_id}" '
                    f'tvg-name="{safe_title}" '
                    f'group-title="{safe_group}" '
                    f'tvg-group="{safe_hierarchical_group}" '
                    f'subgroup-title="{safe_category}"'
                )


                if logo:


                    extinf += (
                        " tvg-logo=\""
                        + cls._m3u_escape(
                            logo
                        )
                        + "\""
                    )


                extinf += (
                    ","
                    + safe_title
                )


                lines.append(
                    extinf
                )


                lines.append(
                    "#EXTVLCOPT:"
                    "http-user-agent="
                    + M3U_USER_AGENT
                )


                lines.append(
                    stream_url
                )


        return (
            "\n".join(
                lines
            )
            + "\n"
        )


    # ========================================================
    # M3U ESCAPE
    # ========================================================


    @staticmethod
    def _m3u_escape(
        value: Any,
    ) -> str:


        return (
            str(value)
            .replace(
                '"',
                "'",
            )
            .replace(
                "\r",
                " ",
            )
            .replace(
                "\n",
                " ",
            )
            .strip()
        )


    # ========================================================
    # TV TXT
    # ========================================================


    @classmethod
    def make_tv_txt(
        cls,
        report: Dict[str, Any],
    ) -> str:


        stats = (
            report[
                "statistics"
            ]
        )


        lines = [
            "=" * 90,


            "RUTUBE TV DISCOVERY",


            "=" * 90,


            f"Version: {VERSION}",


            f"Source: "
            f"{report['meta']['source_url']}",


            f"Generated: "
            f"{report['meta']['generated_at']}",


            "",


            "DISCOVERY",


            "-" * 90,


            f"Discovered records : "
            f"{stats['discovered_records']}",


            f"Processed records  : "
            f"{stats['processed_records']}",


            f"With HLS           : "
            f"{stats['with_hls']}",


            f"Without HLS        : "
            f"{stats['without_hls']}",


            f"Errors             : "
            f"{stats['errors']}",


            f"Total HLS URLs     : "
            f"{stats['total_hls_urls']}",


            "",


            "DEDUPLICATION: DISABLED",


            "",


            "CATEGORIES",


            "-" * 90,
        ]


        for category, values in (
            stats[
                "categories"
            ].items()
        ):


            lines.append(
                (
                    f"{category}: "
                    f"records={values['records']} "
                    f"with_hls={values['with_hls']} "
                    f"hls_urls={values['hls_urls']}"
                )
            )


        lines.extend([
            "",
            "CHANNEL RECORDS",
            "-" * 90,
        ])


        for index, record in enumerate(
            report[
                "channels"
            ],
            start=1,
        ):


            title = (
                record.get(
                    "title"
                )
                or
                record.get(
                    "video_title"
                )
                or
                ""
            )


            category = (
                record.get(
                    "category"
                )
                or
                ""
            )


            video_id = (
                record.get(
                    "video_id"
                )
                or
                ""
            )


            streams = (
                record.get(
                    "hls_streams",
                    [],
                )
            )


            lines.append(
                (
                    f"[{index}] "
                    f"{category} | "
                    f"{title} | "
                    f"ID={video_id} | "
                    f"HLS={len(streams)}"
                )
            )


            for stream in streams:


                lines.append(
                    f"    {stream}"
                )


        lines.extend([
            "",
            "=" * 90,
            "END",
            "=" * 90,
        ])


        return (
            "\n".join(
                lines
            )
            + "\n"
        )


    # ========================================================
    # TXT LEGACY REPORT
    # ========================================================


    @staticmethod
    def make_txt_report(
        report: Dict[str, Any],
    ) -> str:


        video = report[
            "video"
        ]


        stats = report[
            "statistics"
        ]


        streams = report[
            "streams"
        ]


        lines = []


        lines.append(
            "=" * 80
        )


        lines.append(
            "RUTUBE FULL SCRAPER REPORT"
        )


        lines.append(
            "=" * 80
        )


        lines.append(
            f"Report version: "
            f"{report['schema']['version']}"
        )


        lines.append(
            f"Generated: "
            f"{report['meta']['generated_at']}"
        )


        lines.append("")


        lines.append(
            "VIDEO"
        )


        lines.append(
            "-" * 80
        )


        lines.append(
            f"Requested ID : "
            f"{video['requested_id']}"
        )


        lines.append(
            f"Internal ID  : "
            f"{video['internal_id']}"
        )


        lines.append(
            f"Title        : "
            f"{video.get('title') or ''}"
        )


        lines.append(
            f"Author       : "
            f"{video.get('author') or ''}"
        )


        lines.append(
            f"Category     : "
            f"{video.get('category') or ''}"
        )


        lines.append(
            f"Duration     : "
            f"{video.get('duration') or ''}"
        )


        lines.append("")


        lines.append(
            "STREAM STATISTICS"
        )


        lines.append(
            "-" * 80
        )


        lines.append(
            f"Total streams: "
            f"{stats['total_streams']}"
        )


        lines.append(
            f"Alive streams: "
            f"{stats['alive_streams']}"
        )


        lines.append(
            f"Dead streams : "
            f"{stats['dead_streams']}"
        )


        lines.append(
            f"Availability : "
            f"{stats['availability_percent']}%"
        )


        lines.append("")


        lines.append(
            "RESOLUTIONS"
        )


        lines.append(
            "-" * 80
        )


        for resolution, count in sorted(
            stats[
                "resolutions"
            ].items()
        ):


            lines.append(
                f"{resolution:15} "
                f"{count}"
            )


        lines.append("")


        lines.append(
            "STREAMS"
        )


        lines.append(
            "-" * 80
        )


        for index, stream in enumerate(
            streams,
            start=1,
        ):


            lines.append(
                f"[{index}] "
                f"{stream.get('resolution') or '?'} "
                f"| "
                f"{stream.get('bandwidth_kbps') or 0:.0f} kbps "
                f"| "
                f"{stream.get('codecs') or '-'} "
                f"| "
                f"{'ALIVE' if stream.get('alive') else 'DEAD'} "
                f"| "
                f"score={stream.get('quality_score', 0):.2f}"
            )


            lines.append(
                f"    URL: "
                f"{stream.get('url')}"
            )


            lines.append(
                f"    HTTP: "
                f"{stream.get('http_status')}"
            )


            lines.append(
                f"    Response: "
                f"{stream.get('response_time_ms')} ms"
            )


        lines.append("")


        lines.append(
            "=" * 80
        )


        lines.append(
            "END OF REPORT"
        )


        lines.append(
            "=" * 80
        )


        return (
            "\n".join(lines)
            + "\n"
        )


    # ========================================================
    # HELPERS
    # ========================================================


    @staticmethod
    def _json(
        response: requests.Response,
        name: str,
    ) -> Dict[str, Any]:


        try:


            data = response.json()


        except Exception as exc:


            raise RutubeScrapperError(
                f"{name}: invalid JSON: {exc}"
            ) from exc


        if not isinstance(
            data,
            dict,
        ):


            raise RutubeScrapperError(
                f"{name}: expected JSON object"
            )


        return data


    @staticmethod
    def _to_int(
        value: Any,
    ) -> Optional[int]:


        if value is None:
            return None


        try:
            return int(
                float(value)
            )


        except (
            TypeError,
            ValueError,
        ):
            return None


    @staticmethod
    def _to_float(
        value: Any,
    ) -> Optional[float]:


        if value is None:
            return None


        try:
            return float(
                value
            )


        except (
            TypeError,
            ValueError,
        ):
            return None


    @staticmethod
    def _hash_url(
        url: str,
    ) -> str:


        return hashlib.sha256(
            url.encode(
                "utf-8",
                errors="replace",
            )
        ).hexdigest()


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


        if isinstance(
            author,
            str,
        ):


            return author


        return (
            data.get(
                "author_name"
            )
            or
            data.get(
                "user_name"
            )
        )


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
                or author.get("user_id")
            )


            return (
                str(value)
                if value is not None
                else None
            )


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


        if isinstance(
            category,
            str,
        ):


            return category


        return None


    @staticmethod
    def _sanitize_visitor(
        data: Dict[str, Any],
    ) -> Dict[str, Any]:


        sensitive = {
            "club_params_encrypted",
            "token",
            "access_token",
            "refresh_token",
            "password",
        }


        result = {}


        for key, value in (
            data.items()
        ):


            if key.lower() in (
                sensitive
            ):
                continue


            result[key] = value


        return result


    def _sanitize_balancer(
        self,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:


        result = {}


        for key, value in (
            data.items()
        ):


            if key == "m3u8_url":


                result[key] = (
                    self._safe_url(
                        str(value)
                    )
                )


            elif key == "raw":


                result[key] = value


            else:


                result[key] = value


        return result


    @staticmethod
    def _safe_filename_part(
        value: str,
    ) -> str:


        value = re.sub(
            r"[^0-9A-Za-zА-Яа-яЁё_-]+",
            "_",
            str(value),
        )


        value = value.strip(
            "_"
        )


        return (
            value[:80]
            or
            "category"
        )




# ============================================================
# FILE OUTPUT
# ============================================================


def write_json(
    path: Path,
    data: Dict[str, Any],
):


    path.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )




def write_text(
    path: Path,
    text: str,
):


    path.write_text(
        text,
        encoding="utf-8",
    )




def write_jsonl(
    path: Path,
    report: Dict[str, Any],
):


    records = (
        report["ml"]["records"]
    )


    with path.open(
        "w",
        encoding="utf-8",
    ) as f:


        for record in records:


            f.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
            )


            f.write("\n")




def write_tv_jsonl(
    path: Path,
    report: Dict[str, Any],
):


    with path.open(
        "w",
        encoding="utf-8",
    ) as f:


        for record in report[
            "channels"
        ]:


            f.write(
                json.dumps(
                    record,
                    ensure_ascii=False,
                )
            )


            f.write("\n")




# ============================================================
# LOGGING
# ============================================================


def setup_logging(
    output_dir: Path,
):


    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


    log_file = (
        output_dir
        / "scraper.log"
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
                log_file,
                encoding="utf-8",
            ),
        ],
        force=True,
    )




# ============================================================
# TV OUTPUT
# ============================================================


def write_tv_outputs(
    output_dir: Path,
    report: Dict[str, Any],
    scraper: RutubeScrapper,
):


    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


    # --------------------------------------------------------
    # JSON
    # --------------------------------------------------------


    write_json(
        output_dir / JSON_FILENAME,
        report,
    )


    # --------------------------------------------------------
    # M3U
    # --------------------------------------------------------


    m3u_text = (
        scraper.make_tv_m3u(
            report[
                "channels"
            ]
        )
    )


    write_text(
        output_dir / M3U_FILENAME,
        m3u_text,
    )


    # --------------------------------------------------------
    # JSONL
    # --------------------------------------------------------


    write_tv_jsonl(
        output_dir / JSONL_FILENAME,
        report,
    )


    # --------------------------------------------------------
    # TXT
    # --------------------------------------------------------


    write_text(
        output_dir / TXT_FILENAME,
        scraper.make_tv_txt(
            report
        ),
    )


    # --------------------------------------------------------
    # DISCOVERY DEBUG HTML
    #
    # Не обязателен для работы.
    # Оставляем небольшой служебный файл.
    # --------------------------------------------------------


    logging.info(
        "JSON: %s",
        output_dir / JSON_FILENAME,
    )


    logging.info(
        "M3U: %s",
        output_dir / M3U_FILENAME,
    )


    logging.info(
        "JSONL: %s",
        output_dir / JSONL_FILENAME,
    )


    logging.info(
        "TXT: %s",
        output_dir / TXT_FILENAME,
    )




# ============================================================
# LEGACY CLI
# ============================================================


def build_parser():


    parser = argparse.ArgumentParser(
        description=(
            "Rutube Full Scraper + "
            "Autonomous TV Discovery"
        )
    )


    parser.add_argument(
        "video_id",
        nargs="?",
        help=(
            "Optional Rutube video ID. "
            "If omitted, autonomous TV Discovery "
            "is started."
        ),
    )


    parser.add_argument(
        "--phone",
        default=os.getenv(
            "RUTUBE_PHONE"
        ),
        help=(
            "Rutube phone. "
            "Can also be RUTUBE_PHONE"
        ),
    )


    parser.add_argument(
        "--password",
        default=os.getenv(
            "RUTUBE_PASSWORD"
        ),
        help=(
            "Rutube password. "
            "Can also be RUTUBE_PASSWORD"
        ),
    )


    parser.add_argument(
        "--output",
        default=os.getenv(
            "RUTUBE_OUTPUT",
            OUTPUT_DIR,
        ),
        help=(
            "Output directory"
        ),
    )


    parser.add_argument(
        "--no-test",
        action="store_true",
        help=(
            "Do not test individual streams "
            "in legacy video mode"
        ),
    )


    parser.add_argument(
        "--dead-streams",
        action="store_true",
        help=(
            "Include dead streams in legacy M3U"
        ),
    )


    parser.add_argument(
        "--timeout",
        type=float,
        default=float(
            os.getenv(
                "RUTUBE_TIMEOUT",
                "30",
            )
        ),
        help=(
            "HTTP read timeout"
        ),
    )


    parser.add_argument(
        "--max-pages",
        type=int,
        default=int(
            os.getenv(
                "RUTUBE_MAX_PAGES",
                str(
                    DEFAULT_MAX_PAGES
                ),
            )
        ),
        help=(
            "Maximum pages per TV category"
        ),
    )


    parser.add_argument(
        "--max-empty-pages",
        type=int,
        default=int(
            os.getenv(
                "RUTUBE_MAX_EMPTY_PAGES",
                str(
                    DEFAULT_MAX_EMPTY_PAGES
                ),
            )
        ),
        help=(
            "Maximum consecutive empty "
            "pages per category"
        ),
    )


    parser.add_argument(
        "--insecure",
        action="store_true",
        help=(
            "Disable SSL verification"
        ),
    )


    return parser




# ============================================================
# AUTONOMOUS TV MAIN
# ============================================================


def autonomous_tv_main(
    args,
):


    output_dir = Path(
        args.output
    )


    setup_logging(
        output_dir
    )


    logging.info(
        "=" * 80
    )


    logging.info(
        "RUTUBE TV AUTONOMOUS DISCOVERY"
    )


    logging.info(
        "VERSION: %s",
        VERSION,
    )


    logging.info(
        "SOURCE: %s",
        DISCOVERY_SOURCE_URL,
    )


    logging.info(
        "TV PROGRAM SOURCE: %s",
        TV_PROGRAM_SOURCE_URL,
    )


    logging.info(
        "DEDUPLICATION: DISABLED"
    )


    logging.info(
        "=" * 80
    )


    scraper = RutubeScrapper(
        timeout=(
            10,
            args.timeout,
        ),
        verify_ssl=(
            not args.insecure
        ),
    )


    try:


        report = (
            scraper.scrape_tv_catalog(
                source_url=(
                    DISCOVERY_SOURCE_URL
                ),
                max_pages=(
                    max(
                        1,
                        args.max_pages
                    )
                ),
                max_empty_pages=(
                    max(
                        1,
                        args.max_empty_pages
                    )
                ),
            )
        )


        write_tv_outputs(
            output_dir,
            report,
            scraper,
        )


        stats = report[
            "statistics"
        ]


        print()
        print(
            "=" * 80
        )


        print(
            "RUTUBE TV DISCOVERY FINISHED"
        )


        print(
            "=" * 80
        )


        print(
            "Deduplication    : DISABLED"
        )


        print(
            "Records found    : "
            f"{stats['discovered_records']}"
        )


        print(
            "Records processed: "
            f"{stats['processed_records']}"
        )


        print(
            "With HLS         : "
            f"{stats['with_hls']}"
        )


        print(
            "Without HLS      : "
            f"{stats['without_hls']}"
        )


        print(
            "Errors           : "
            f"{stats['errors']}"
        )


        print(
            "HLS URLs         : "
            f"{stats['total_hls_urls']}"
        )


        print()


        print(
            "M3U              : "
            f"{output_dir / M3U_FILENAME}"
        )


        print(
            "JSON             : "
            f"{output_dir / JSON_FILENAME}"
        )


        print(
            "JSONL            : "
            f"{output_dir / JSONL_FILENAME}"
        )


        print(
            "TXT              : "
            f"{output_dir / TXT_FILENAME}"
        )


        print(
            "LOG              : "
            f"{output_dir / 'scraper.log'}"
        )


        print(
            "=" * 80
        )


        return 0


    except KeyboardInterrupt:


        logging.error(
            "Interrupted by user"
        )


        return 130


    except Exception as exc:


        logging.exception(
            "Autonomous discovery failed: %s",
            exc,
        )


        return 1




# ============================================================
# LEGACY SINGLE VIDEO MAIN
# ============================================================


def legacy_video_main(
    args,
):


    if not args.phone:


        print(
            "ERROR: Rutube phone is required "
            "for legacy single-video mode.",
            file=sys.stderr,
        )


        return 2


    if not args.password:


        print(
            "ERROR: Rutube password is required "
            "for legacy single-video mode.",
            file=sys.stderr,
        )


        return 2


    output_dir = Path(
        args.output
    )


    setup_logging(
        output_dir
    )


    logging.info(
        "Starting Rutube legacy scraper %s",
        VERSION,
    )


    scraper = RutubeScrapper(
        timeout=(
            10,
            args.timeout,
        ),
        verify_ssl=(
            not args.insecure
        ),
    )


    try:


        report = scraper.scrape(
            video_id=args.video_id,
            phone=args.phone,
            password=args.password,
            test_streams=(
                not args.no_test
            ),
        )


        video = report[
            "video"
        ]


        internal_id = (
            video[
                "internal_id"
            ]
        )


        base_name = (
            f"rutube_"
            f"{internal_id}"
        )


        # ----------------------------------------------------
        # JSON
        # ----------------------------------------------------


        json_path = (
            output_dir
            / f"{base_name}.json"
        )


        write_json(
            json_path,
            report,
        )


        # ----------------------------------------------------
        # TXT
        # ----------------------------------------------------


        txt_path = (
            output_dir
            / f"{base_name}.txt"
        )


        write_text(
            txt_path,
            scraper.make_txt_report(
                report
            ),
        )


        # ----------------------------------------------------
        # M3U
        # ----------------------------------------------------


        m3u_path = (
            output_dir
            / f"{base_name}.m3u"
        )


        streams = [
            StreamInfo(
                **stream
            )
            for stream
            in report[
                "streams"
            ]
        ]


        m3u_text = (
            scraper.make_m3u(
                VideoInfo(
                    **{
                        key:
                            video[key]
                        for key
                        in
                        VideoInfo.__dataclass_fields__
                        if key in video
                    }
                ),
                streams,
                only_alive=(
                    not args.dead_streams
                ),
            )
        )


        write_text(
            m3u_path,
            m3u_text,
        )


        # ----------------------------------------------------
        # ML JSONL
        # ----------------------------------------------------


        jsonl_path = (
            output_dir
            / f"{base_name}_ml.jsonl"
        )


        write_jsonl(
            jsonl_path,
            report,
        )


        # ----------------------------------------------------
        # RAW M3U8
        # ----------------------------------------------------


        raw_m3u8_path = (
            output_dir
            / f"{base_name}_master.m3u8"
        )


        try:


            raw_url = (
                report[
                    "stream_source"
                ][
                    "balancer_url"
                ]
            )


            # Отчёт содержит безопасный URL,
            # поэтому повторно получаем master только
            # при возможности.
            #
            # Это не влияет на основной результат.
            if raw_url:


                raw_text = (
                    scraper.get_m3u8(
                        raw_url
                    )
                )


                write_text(
                    raw_m3u8_path,
                    raw_text,
                )


        except Exception as exc:


            logging.warning(
                "RAW M3U8 save failed: %s",
                exc,
            )


        # ----------------------------------------------------
        # SUMMARY
        # ----------------------------------------------------


        stats = report[
            "statistics"
        ]


        print()
        print(
            "=" * 70
        )


        print(
            "RUTUBE SCRAPER FINISHED"
        )


        print(
            "=" * 70
        )


        print(
            f"Video ID       : "
            f"{video['requested_id']}"
        )


        print(
            f"Internal ID    : "
            f"{video['internal_id']}"
        )


        print(
            f"Title          : "
            f"{video.get('title') or ''}"
        )


        print(
            f"Streams        : "
            f"{stats['total_streams']}"
        )


        print(
            f"Alive          : "
            f"{stats['alive_streams']}"
        )


        print(
            f"Dead           : "
            f"{stats['dead_streams']}"
        )


        print(
            f"Availability   : "
            f"{stats['availability_percent']}%"
        )


        print()


        print(
            f"JSON           : "
            f"{json_path}"
        )


        print(
            f"TXT            : "
            f"{txt_path}"
        )


        print(
            f"M3U            : "
            f"{m3u_path}"
        )


        print(
            f"ML JSONL       : "
            f"{jsonl_path}"
        )


        print(
            f"RAW M3U8       : "
            f"{raw_m3u8_path}"
        )


        print(
            "=" * 70
        )


        return 0


    except RutubeScrapperError as exc:


        logging.error(
            "Rutube error: %s",
            exc,
        )


        return 2


    except KeyboardInterrupt:


        logging.error(
            "Interrupted by user"
        )


        return 130


    except Exception as exc:


        logging.exception(
            "Unexpected error: %s",
            exc,
        )


        return 1




# ============================================================
# MAIN
# ============================================================


def main():


    parser = build_parser()


    args = parser.parse_args()


    # ========================================================
    # АВТОНОМНЫЙ РЕЖИМ
    # ========================================================
    #
    # Просто:
    #
    #     python m3u_rutube.py
    #
    # Никакого video_id.
    # Никаких обязательных credentials.
    #
    # ========================================================


    if not args.video_id:


        return autonomous_tv_main(
            args
        )


    # ========================================================
    # LEGACY MODE
    # ========================================================


    return legacy_video_main(
        args
    )




# ============================================================
# ENTRY POINT
# ============================================================


if __name__ == "__main__":


    sys.exit(
        main()
    )