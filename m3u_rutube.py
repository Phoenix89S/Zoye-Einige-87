#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Rutube Full Scraper / Analyzer

Возможности:
    - авторизация Rutube LiST
    - получение internal video ID
    - получение visitor information
    - получение award
    - получение video balancer
    - получение master M3U8
    - разбор всех HLS вариантов
    - получение дополнительной информации о каждом stream
    - JSON полный отчёт
    - TXT человекочитаемый отчёт
    - M3U playlist
    - ML-oriented JSONL dataset
    - ML feature vectors
    - статистика
    - безопасное логирование без пароля/cookies/award
    - retry / timeout
    - HTTP status validation
    - URL normalization
    - сохранение raw M3U8
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time

from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter

try:
    from urllib3.util.retry import Retry
except ImportError:
    Retry = None


# ============================================================
# VERSION
# ============================================================

VERSION = "2.0.0-ML"


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
        })

        self.request_stats: List[RequestStat] = []

        self.started_at = None
        self.finished_at = None

        self._configure_retry(retries)

    # --------------------------------------------------------
    # HTTP
    # --------------------------------------------------------

    def _configure_retry(self, retries: int):

        if Retry is None:
            return

        retry = Retry(
            total=retries,
            connect=retries,
            read=retries,
            status=retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({
                "GET",
                "HEAD",
                "OPTIONS",
            }),
            raise_on_status=False,
        )

        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=20,
            pool_maxsize=20,
        )

        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

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
                time.perf_counter() - started
            ) * 1000

            stat.status = response.status_code
            stat.ok = response.ok
            stat.duration_ms = round(elapsed, 3)

            try:
                stat.response_size = len(response.content)
            except Exception:
                pass

            self.request_stats.append(stat)

            response.raise_for_status()

            return response

        except Exception as exc:

            elapsed = (
                time.perf_counter() - started
            ) * 1000

            stat.duration_ms = round(elapsed, 3)
            stat.error = str(exc)

            self.request_stats.append(stat)

            raise RutubeScrapperError(
                f"{name}: {exc}"
            ) from exc

    # --------------------------------------------------------
    # SECURITY
    # --------------------------------------------------------

    @staticmethod
    def _safe_url(url: str) -> str:

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
                    query[key] = ["<REDACTED>"]

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

        if not phone.strip() or not password.strip():
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

        result = self._json(response, "login")

        if result.get("success") is not True:
            raise RutubeScrapperError(
                "Invalid credentials"
            )

        # В оригинальном PHP это действие необходимо
        # для получения дополнительных cookies.
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

        info = VideoInfo(
            requested_id=str(video_id),
            internal_id=str(internal_id),
            title=data.get("title"),
            description=data.get("description"),
            duration=self._to_float(
                data.get("duration")
            ),
            author=self._extract_author(data),
            author_id=self._extract_author_id(data),
            category=self._extract_category(data),
            created_at=data.get("created_ts")
            or data.get("created_at"),
            published_at=data.get("publication_date")
            or data.get("published_at"),
            raw=data,
        )

        return info

    # ========================================================
    # VISITOR
    # ========================================================

    def visitor(self) -> Dict[str, Any]:

        response = self._request(
            "GET",
            self.endpoints["visitor_api"],
            "visitor",
        )

        data = self._json(
            response,
            "visitor",
        )

        return data

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
            "video_id": internal_video_id,
        }

        # В PHP исходная строка строится как:
        #
        # ?{$clubParamsEncrypted}&video_id=...
        #
        # Поэтому encrypted params могут сами содержать
        # несколько параметров.
        separator = "&" if "?" in club_params else "?"

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

        award_value = data.get("award")

        if not award_value:
            raise RutubeScrapperError(
                "Award token was not returned"
            )

        return {
            "award": award_value,
            "visitor": self._sanitize_visitor(
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
            dict
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
            "raw": data,
            "video_balancer": video_balancer,
            "m3u8_url": m3u8_url,
        }

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

        for index, line in enumerate(lines):

            if not line.startswith(
                "#EXT-X-STREAM-INF:"
            ):
                continue

            attributes_text = line[
                len("#EXT-X-STREAM-INF:"):
            ]

            attributes = self._parse_m3u8_attributes(
                attributes_text
            )

            stream_url = None

            for next_line in lines[index + 1:]:

                if next_line.startswith("#"):
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
                    width = int(match.group(1))
                    height = int(match.group(2))

            bandwidth = self._to_int(
                attributes.get("BANDWIDTH")
            )

            average_bandwidth = self._to_int(
                attributes.get(
                    "AVERAGE-BANDWIDTH"
                )
            )

            frame_rate = self._to_float(
                attributes.get("FRAME-RATE")
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
                average_bandwidth=average_bandwidth,
                codecs=attributes.get("CODECS"),
                mime_type=attributes.get(
                    "MIME-TYPE"
                ),
                frame_rate=frame_rate,
                audio=attributes.get("AUDIO"),
                video=attributes.get("VIDEO"),
                url=stream_url,
                url_hash=self._hash_url(
                    stream_url
                ),
                source_index=len(streams),
                raw_attributes=attributes,
            )

            stream.quality_score = (
                self.calculate_quality_score(
                    stream
                )
            )

            streams.append(stream)

        return streams

    # ========================================================
    # M3U8 ATTRIBUTE PARSER
    # ========================================================

    @staticmethod
    def _parse_m3u8_attributes(
        text: str,
    ) -> Dict[str, str]:

        result = {}

        # Поддержка:
        #
        # KEY=VALUE
        # KEY="VALUE,WITH,COMMAS"
        #
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

        for match in pattern.finditer(text):

            key = match.group(1)

            value = (
                match.group(2)
                if match.group(2) is not None
                else match.group(3)
            )

            result[key] = value.strip()

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

        # Alive/dead корректирует итоговую оценку.
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

        # Разрешение
        if stream.width and stream.height:

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

        # Bandwidth
        if stream.bandwidth:

            score += min(
                25,
                stream.bandwidth / 400_000,
            )

        # FPS
        if stream.frame_rate:

            if stream.frame_rate >= 50:
                score += 10

            elif stream.frame_rate >= 25:
                score += 7

            elif stream.frame_rate >= 20:
                score += 4

        # Alive
        if stream.alive is True:
            score += 15

        elif stream.alive is False:
            score -= 20

        return round(
            max(0.0, min(score, 100.0)),
            3,
        )

    # ========================================================
    # FULL SCRAPE
    # ========================================================

    def scrape(
        self,
        video_id: str,
        phone: str,
        password: str,
        test_streams: bool = True,
    ) -> Dict[str, Any]:

        self.started_at = datetime.now(
            timezone.utc
        ).isoformat()

        video = self.video_after_login(
            video_id,
            phone,
            password,
        )

        award_data = self.award(
            video.internal_id
        )

        award_value = award_data["award"]

        balancer = self.get_balancer(
            video.internal_id,
            award_value,
        )

        m3u8_url = balancer[
            "m3u8_url"
        ]

        m3u8_text = self.get_m3u8(
            m3u8_url
        )

        streams = self.parse_m3u8(
            m3u8_text
        )

        if test_streams:

            for stream in streams:
                self.test_stream(stream)

        self.finished_at = datetime.now(
            timezone.utc
        ).isoformat()

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
                "name": "RutubeMLVideoReport",
                "version": VERSION,
            },

            "meta": {
                "generated_at": datetime.now(
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

            "video": asdict(video),

            "stream_source": {
                "balancer_url": self._safe_url(
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

            "http":
                {
                    "requests":
                        [
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
                resolutions.get(key, 0)
                + 1
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
                "width": "integer",
                "height": "integer",
                "pixels": "integer",
                "bandwidth": "integer",
                "bandwidth_kbps": "float",
                "average_bandwidth": "integer",
                "frame_rate": "float",
                "alive": "binary",
                "http_status": "integer",
                "response_time_ms": "float",
                "quality_score": "float",
                "has_audio": "binary",
                "has_video": "binary",
                "has_codecs": "binary",
            },

            "records":
                records,
        }

    # ========================================================
    # M3U PLAYLIST
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

            title = (
                f"{title} | "
                f"{resolution} | "
                f"{stream.bandwidth_kbps:.0f} kbps"
                if stream.bandwidth_kbps
                else
                f"{title} | {resolution}"
            )

            output.append(
                (
                    f'#EXTINF:-1 '
                    f'tvg-id="rutube_{video.internal_id}_{index}" '
                    f'tvg-name="{title}",'
                    f'{title}'
                )
            )

            output.append(
                stream.url
            )

        return "\n".join(output) + "\n"

    # ========================================================
    # TXT REPORT
    # ========================================================

    @staticmethod
    def make_txt_report(
        report: Dict[str, Any],
    ) -> str:

        video = report["video"]
        stats = report["statistics"]
        streams = report["streams"]

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
            stats["resolutions"].items()
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

        return "\n".join(lines) + "\n"

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

        if not isinstance(data, dict):

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
            return int(float(value))

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
            return float(value)

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

        author = data.get("author")

        if isinstance(author, dict):

            return (
                author.get("name")
                or author.get("title")
                or author.get("username")
            )

        if isinstance(author, str):
            return author

        return (
            data.get("author_name")
            or data.get("user_name")
        )

    @staticmethod
    def _extract_author_id(
        data: Dict[str, Any],
    ) -> Optional[str]:

        author = data.get("author")

        if isinstance(author, dict):

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

        if isinstance(category, dict):

            return (
                category.get("name")
                or category.get("title")
            )

        if isinstance(category, str):
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

        for key, value in data.items():

            if key.lower() in sensitive:
                continue

            result[key] = value

        return result

    def _sanitize_balancer(
        self,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:

        result = {}

        for key, value in data.items():

            if key == "m3u8_url":

                result[key] = self._safe_url(
                    str(value)
                )

            elif key == "raw":

                result[key] = value

            else:
                result[key] = value

        return result


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
    )


# ============================================================
# CLI
# ============================================================

def build_parser():

    parser = argparse.ArgumentParser(
        description=(
            "Rutube Full Scraper + "
            "JSON/TXT/M3U/ML reports"
        )
    )

    parser.add_argument(
        "video_id",
        help=(
            "Rutube video ID or short video ID"
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
        default="rutube_output",
        help=(
            "Output directory"
        ),
    )

    parser.add_argument(
        "--no-test",
        action="store_true",
        help=(
            "Do not test individual streams"
        ),
    )

    parser.add_argument(
        "--dead-streams",
        action="store_true",
        help=(
            "Include dead streams in M3U"
        ),
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help=(
            "HTTP read timeout"
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
# MAIN
# ============================================================

def main():

    parser = build_parser()
    args = parser.parse_args()

    if not args.phone:
        parser.error(
            "Rutube phone is required"
        )

    if not args.password:
        parser.error(
            "Rutube password is required"
        )

    output_dir = Path(
        args.output
    )

    setup_logging(
        output_dir
    )

    logging.info(
        "Starting Rutube scraper %s",
        VERSION,
    )

    scraper = RutubeScrapper(
        timeout=(
            10,
            args.timeout,
        ),
        verify_ssl=not args.insecure,
    )

    try:

        report = scraper.scrape(
            video_id=args.video_id,
            phone=args.phone,
            password=args.password,
            test_streams=not args.no_test,
        )

        video = report["video"]

        internal_id = (
            video["internal_id"]
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
            in report["streams"]
        ]

        m3u_text = (
            scraper.make_m3u(
                VideoInfo(
                    **{
                        key: video[key]
                        for key
                        in VideoInfo.__dataclass_fields__
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

        # M3U8 не находится в report целиком,
        # поэтому повторно получаем URL.
        #
        # В production-версии его лучше хранить
        # непосредственно в объекте результата.

        balancer_url = (
            report[
                "stream_source"
            ][
                "balancer_url"
            ]
        )

        logging.info(
            "JSON: %s",
            json_path,
        )

        logging.info(
            "TXT: %s",
            txt_path,
        )

        logging.info(
            "M3U: %s",
            m3u_path,
        )

        logging.info(
            "ML JSONL: %s",
            jsonl_path,
        )

        # ----------------------------------------------------
        # CONSOLE SUMMARY
        # ----------------------------------------------------

        stats = report[
            "statistics"
        ]

        print()
        print("=" * 70)
        print("RUTUBE SCRAPER FINISHED")
        print("=" * 70)

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

        print("=" * 70)

    except RutubeScrapperError as exc:

        logging.error(
            "Rutube error: %s",
            exc,
        )

        sys.exit(2)

    except KeyboardInterrupt:

        logging.error(
            "Interrupted by user"
        )

        sys.exit(130)

    except Exception as exc:

        logging.exception(
            "Unexpected error: %s",
            exc,
        )

        sys.exit(1)


if __name__ == "__main__":
    main()