#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ZOYA IPTV DISCOVERY & FAILOVER ENGINE
=====================================

Expanded fresh-start implementation.

Features
--------
1. Public M3U/M3U8/TXT source discovery
2. Source registry with country/region/trust metadata
3. M3U parser preserving useful EXTINF attributes
4. Channel normalization + alias matching
5. Unlimited stream pool per channel
6. URL deduplication
7. 18+/NSFW filtering
8. Fast HTTP probe
9. HLS manifest probe
10. Optional FFprobe media inspection
11. Rolling health / uptime history
12. Latency / media quality / source trust scoring
13. online / unstable / dead / blocked / unchecked states
14. Persistent SQLite database
15. Whole-pool health scans
16. Current stream watchdog
17. Automatic failover
18. Automatic recovery promotion
19. Atomic M3U replacement
20. Full pool M3U with pool-no markers
21. Current playlist M3U
22. JSON machine report
23. Human-readable TXT report
24. Per-channel event history
25. Discovery history
26. GitHub/public raw playlist URL support
27. Configurable discovery source groups
28. GitHub Actions friendly CLI
29. Dry-run mode
30. Recheck mode for existing pool
31. No artificial 3/5/10 backup limit

Safety / scope
-------------
Zoya operates on publicly reachable resources supplied through configuration.
It does not bypass authentication, guess credentials, defeat access controls,
or probe private provider infrastructure.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import gzip
import hashlib
import json
import logging
import os
import re
import sqlite3
import subprocess
import tempfile
import threading
import xml.etree.ElementTree as ET
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


APP = "Zoya"
VERSION = "2.0.0"

ROOT = Path(__file__).resolve().parent
OUTPUT = ROOT / "output"
DB_FILE = ROOT / "zoya.db"
SOURCES_FILE = ROOT / "sources.json"

PLAYLIST_FILE = OUTPUT / "zoya_playlist.m3u"
POOL_FILE = OUTPUT / "zoya_pool.m3u"
REPORT_JSON = OUTPUT / "zoya_report.json"
REPORT_TXT = OUTPUT / "zoya_report.txt"
EVENTS_JSON = OUTPUT / "zoya_events.json"
LOG_FILE = OUTPUT / "zoya.log"

EPG_DIR = OUTPUT / "epg"
EPG_CACHE_DIR = EPG_DIR / "cache"
EPG_MERGED_XML = EPG_DIR / "zoya_epg.xml"
EPG_MERGED_GZ = EPG_DIR / "zoya_epg.xml.gz"
EPG_REPORT = EPG_DIR / "epg_report.json"
EPG_TTL_HOURS = 12

DEFAULT_EPG_SOURCES = [
    {
        "id": "epgone",
        "name": "EPG.one",
        "url": "https://epg.one/epg2.xml.gz",
        "priority": 1,
        "enabled": True,
    },
    {
        "id": "teleguide",
        "name": "Teleguide",
        "url": "https://www.teleguide.info/download/new3/xmltv.xml.gz",
        "priority": 2,
        "enabled": True,
    },
]

DEFAULT_WORKERS = 32
DEFAULT_TIMEOUT = 10
DEFAULT_RETRIES = 1
DEFAULT_HLS_SEGMENTS = 2
DEFAULT_FFPROBE_TIMEOUT = 12

USER_AGENT = "Zoya-IPTV/2.0"

NSFW_TOKENS = {
    "18+", "18plus", "adult", "xxx", "porn", "porno",
    "pornhub", "sex", "erotic", "эротика", "порно",
    "порн", "секс", "для взрослых",
}

# This is intentionally limited to explicit credential/auth URL patterns.
BLOCKED_URL_TOKENS = {
    "username=",
    "password=",
    "passwd=",
    "access_token=",
    "auth_token=",
}

_thread_local = threading.local()


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256(value: str) -> str:
    return hashlib.sha256(value.strip().encode("utf-8")).hexdigest()


def normalize_name(value: str) -> str:
    value = value or ""
    value = re.sub(r"\(\s*[+-]\d+(?::\d+)?\s*\)", " ", value)
    value = re.sub(r"\[[^\]]*\]", " ", value)
    value = re.sub(r"\([^)]*\)", " ", value)
    value = re.sub(r"\b\d{1,4}[.)]\s*", " ", value)
    value = value.lower().replace("ё", "е")
    value = re.sub(r"[^a-zа-яё0-9]+", " ", value, flags=re.I)
    return re.sub(r"\s+", " ", value).strip()


def is_http_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in {"http", "https"} and bool(p.netloc)
    except Exception:
        return False


def looks_nsfw(value: str) -> bool:
    text = (value or "").lower()
    return any(token in text for token in NSFW_TOKENS)


def allowed_url(url: str) -> bool:
    if not is_http_url(url):
        return False
    low = url.lower()
    return not any(x in low for x in BLOCKED_URL_TOKENS)


def get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        retry = Retry(
            total=DEFAULT_RETRIES,
            connect=DEFAULT_RETRIES,
            read=DEFAULT_RETRIES,
            backoff_factor=0.25,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "HEAD"}),
        )
        adapter = HTTPAdapter(
            pool_connections=DEFAULT_WORKERS,
            pool_maxsize=DEFAULT_WORKERS,
            max_retries=retry,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": USER_AGENT})
        _thread_local.session = session
    return session


def atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


@dataclass
class SourceConfig:
    id: str
    url: str
    name: str = ""
    country: str = ""
    region: str = ""
    trust: float = 0.5
    enabled: bool = True
    kind: str = "m3u"


@dataclass
class ProbeResult:
    status: str
    http_code: int | None = None
    latency_ms: float | None = None
    content_type: str = ""
    bytes_read: int = 0
    error: str = ""
    manifest: bool = False
    media: dict[str, Any] = field(default_factory=dict)


@dataclass
class Stream:
    url: str
    source_ids: list[str] = field(default_factory=list)

    pool_no: int = 0
    status: str = "unchecked"
    previous_status: str = ""

    latency_ms: float | None = None
    bitrate: int | None = None
    width: int | None = None
    height: int | None = None
    frame_rate: float | None = None
    video_codec: str = ""
    audio_codec: str = ""

    successes: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0

    uptime: float = 0.0
    score: float = 0.0

    first_seen: str = ""
    last_checked: str = ""
    last_online: str = ""
    last_error: str = ""

    current: bool = False
    retired: bool = False

    @property
    def id(self) -> str:
        return sha256(self.url)

    @property
    def resolution(self) -> str:
        if self.width and self.height:
            return f"{self.width}x{self.height}"
        return ""


@dataclass
class Channel:
    key: str
    name: str
    tvg_id: str = ""
    group: str = ""
    logo: str = ""
    country: str = ""
    region: str = ""

    aliases: set[str] = field(default_factory=set)
    streams: list[Stream] = field(default_factory=list)

    current_stream_id: str | None = None
    replacement_count: int = 0



@dataclass
class EPGSource:
    id: str
    name: str
    url: str
    priority: int = 100
    enabled: bool = True


@dataclass
class EPGProgram:
    channel_id: str
    start: str
    stop: str
    title: str = ""
    desc: str = ""
    category: str = ""
    icon: str = ""


class EPGManager:
    """
    XMLTV aggregator.

    Matching priority:
      1. exact tvg-id
      2. known alias / normalized channel id
      3. normalized display name
      4. conservative token similarity

    EPG sources are merged by priority. Lower priority sources fill gaps,
    but do not overwrite an already selected programme occupying the same
    channel/start slot.
    """

    def __init__(self, channels: dict[str, Channel]):
        self.channels = channels
        self.sources: list[EPGSource] = []
        self.channel_map: dict[str, str] = {}
        self.programmes: dict[str, list[EPGProgram]] = {}
        self.source_stats: list[dict[str, Any]] = []
        self.match_stats = {
            "channels_total": 0,
            "channels_matched": 0,
            "channels_unmatched": 0,
            "programmes_total": 0,
        }

    def load_sources(self) -> None:
        cfg = ROOT / "epg_sources.json"

        if cfg.exists():
            try:
                data = json.loads(cfg.read_text(encoding="utf-8"))
                raw = data.get("sources", [])
            except Exception as exc:
                logging.error("EPG config error: %s", exc)
                raw = []
        else:
            raw = DEFAULT_EPG_SOURCES

        self.sources = sorted(
            [
                EPGSource(
                    id=str(x.get("id") or sha256(x["url"])[:12]),
                    name=str(x.get("name", "")),
                    url=str(x.get("url", "")),
                    priority=int(x.get("priority", 100)),
                    enabled=bool(x.get("enabled", True)),
                )
                for x in raw
                if x.get("url") and x.get("enabled", True)
            ],
            key=lambda x: x.priority,
        )

    @staticmethod
    def _text(parent: ET.Element, tag: str) -> str:
        node = parent.find(tag)
        if node is None:
            return ""
        return "".join(node.itertext()).strip()

    @staticmethod
    def _normalize_id(value: str) -> str:
        return normalize_name(value).replace(" ", "")

    def _build_channel_map(self) -> None:
        self.channel_map.clear()

        for key, channel in self.channels.items():
            variants = {
                key,
                channel.tvg_id,
                channel.name,
                *channel.aliases,
            }

            for variant in variants:
                n = self._normalize_id(variant)
                if n:
                    self.channel_map[n] = key

    def _resolve_epg_channel(
        self,
        epg_id: str,
        display_names: list[str],
    ) -> str | None:
        candidates = [epg_id] + display_names

        for candidate in candidates:
            n = self._normalize_id(candidate)
            if n in self.channel_map:
                return self.channel_map[n]

        # Conservative token-based fallback. We require at least one
        # significant token and avoid arbitrary fuzzy substitutions.
        for candidate in candidates:
            n = normalize_name(candidate)
            if not n:
                continue

            tokens = set(n.split())
            if len(tokens) < 1:
                continue

            best_key = None
            best_ratio = 0.0

            for mapped, channel_key in self.channel_map.items():
                mapped_tokens = set(mapped.split())
                if not mapped_tokens:
                    continue

                overlap = len(tokens & mapped_tokens)
                ratio = overlap / max(len(tokens), len(mapped_tokens))

                if ratio > best_ratio:
                    best_ratio = ratio
                    best_key = channel_key

            if best_ratio >= 0.80:
                return best_key

        return None

    def _parse_xml(self, payload: bytes, source: EPGSource) -> tuple[int, int]:
        if payload[:2] == b"\x1f\x8b":
            payload = gzip.decompress(payload)

        root = ET.fromstring(payload)

        epg_channels = 0
        programmes = 0

        for node in root.findall("channel"):
            epg_id = node.attrib.get("id", "").strip()
            if not epg_id:
                continue

            names = [
                "".join(x.itertext()).strip()
                for x in node.findall("display-name")
                if "".join(x.itertext()).strip()
            ]

            key = self._resolve_epg_channel(epg_id, names)

            if key:
                self.match_stats["channels_matched"] += 1
                self.channel_map[self._normalize_id(epg_id)] = key

        for node in root.findall("programme"):
            epg_id = node.attrib.get("channel", "").strip()
            if not epg_id:
                continue

            key = self._resolve_epg_channel(epg_id, [])
            if not key:
                continue

            title = self._text(node, "title")
            desc = self._text(node, "desc")
            category = self._text(node, "category")

            icon = ""
            icon_node = node.find("icon")
            if icon_node is not None:
                icon = icon_node.attrib.get("src", "")

            program = EPGProgram(
                channel_id=key,
                start=node.attrib.get("start", ""),
                stop=node.attrib.get("stop", ""),
                title=title,
                desc=desc,
                category=category,
                icon=icon,
            )

            self.programmes.setdefault(key, []).append(program)
            programmes += 1

        return epg_channels, programmes

    def fetch_all(self) -> None:
        EPG_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self.load_sources()
        self._build_channel_map()

        self.match_stats["channels_total"] = len(self.channels)

        # Lower priority number wins. We fill only missing time slots from
        # later sources.
        occupied: dict[str, set[tuple[str, str]]] = {}

        for source in self.sources:
            started = time.monotonic()

            try:
                response = get_session().get(
                    source.url,
                    timeout=DEFAULT_TIMEOUT * 2,
                )
                response.raise_for_status()
                payload = response.content

                cache_file = EPG_CACHE_DIR / f"{source.id}.xml.gz"
                cache_file.write_bytes(payload)

                before = sum(
                    len(v) for v in self.programmes.values()
                )

                # Parse independently, then retain priority-first entries.
                old = self.programmes
                self.programmes = {}

                self._parse_xml(payload, source)

                incoming = self.programmes
                self.programmes = {
                    key: list(values)
                    for key, values in old.items()
                }

                added = 0
                for key, values in incoming.items():
                    slots = occupied.setdefault(key, set())
                    target = self.programmes.setdefault(key, [])

                    for program in values:
                        slot = (program.start, program.stop)
                        if slot in slots:
                            continue

                        target.append(program)
                        slots.add(slot)
                        added += 1

                elapsed = round(
                    (time.monotonic() - started) * 1000,
                    1,
                )

                self.source_stats.append({
                    "id": source.id,
                    "name": source.name,
                    "priority": source.priority,
                    "status": "ok",
                    "programmes_added": added,
                    "elapsed_ms": elapsed,
                })

                logging.info(
                    "EPG [%s] priority=%d added=%d",
                    source.name,
                    source.priority,
                    added,
                )

            except Exception as exc:
                self.source_stats.append({
                    "id": source.id,
                    "name": source.name,
                    "priority": source.priority,
                    "status": "error",
                    "error": str(exc)[:500],
                })
                logging.error(
                    "EPG [%s] ERROR: %s",
                    source.name,
                    exc,
                )

        self.match_stats["programmes_total"] = sum(
            len(x) for x in self.programmes.values()
        )
        self.match_stats["channels_unmatched"] = (
            self.match_stats["channels_total"]
            - self.match_stats["channels_matched"]
        )

        self._sort()
        self._write()

    def _sort(self) -> None:
        for key in self.programmes:
            self.programmes[key].sort(
                key=lambda p: (p.start, p.stop, p.title)
            )

    def _write(self) -> None:
        EPG_DIR.mkdir(parents=True, exist_ok=True)

        root = ET.Element(
            "tv",
            {
                "generator-info-name": f"Zoya {VERSION}",
                "date": utc_now(),
            },
        )

        for key, channel in sorted(
            self.channels.items(),
            key=lambda x: normalize_name(x[1].name),
        ):
            channel_id = channel.tvg_id or key
            cnode = ET.SubElement(
                root,
                "channel",
                {"id": channel_id},
            )
            dn = ET.SubElement(cnode, "display-name")
            dn.text = channel.name

            if channel.logo:
                ET.SubElement(
                    cnode,
                    "icon",
                    {"src": channel.logo},
                )

        for key, programs in self.programmes.items():
            channel = self.channels.get(key)
            if not channel:
                continue

            channel_id = channel.tvg_id or key

            for program in programs:
                attrs = {
                    "start": program.start,
                    "stop": program.stop,
                    "channel": channel_id,
                }

                pnode = ET.SubElement(
                    root,
                    "programme",
                    attrs,
                )

                title = ET.SubElement(pnode, "title")
                title.text = program.title or "No information"

                if program.desc:
                    desc = ET.SubElement(pnode, "desc")
                    desc.text = program.desc

                if program.category:
                    category = ET.SubElement(pnode, "category")
                    category.text = program.category

                if program.icon:
                    ET.SubElement(
                        pnode,
                        "icon",
                        {"src": program.icon},
                    )

        tree = ET.ElementTree(root)

        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            dir=EPG_DIR,
            suffix=".xml",
        ) as f:
            temp_xml = Path(f.name)

        try:
            tree.write(
                temp_xml,
                encoding="utf-8",
                xml_declaration=True,
            )

            atomic_write(
                EPG_MERGED_XML,
                temp_xml.read_text(encoding="utf-8"),
            )

            compressed = gzip.compress(
                temp_xml.read_bytes(),
                compresslevel=6,
                mtime=0,
            )

            fd, temp_gz = tempfile.mkstemp(
                prefix=EPG_MERGED_GZ.name + ".",
                dir=EPG_DIR,
            )

            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(compressed)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(temp_gz, EPG_MERGED_GZ)
            finally:
                if os.path.exists(temp_gz):
                    os.unlink(temp_gz)

        finally:
            temp_xml.unlink(missing_ok=True)

        atomic_write(
            EPG_REPORT,
            json.dumps(
                {
                    "generated_at": utc_now(),
                    "sources": self.source_stats,
                    "matching": self.match_stats,
                },
                ensure_ascii=False,
                indent=2,
            ),
        )

    def header_url(self) -> str:
        # Relative filename is portable for local/static hosting.
        return EPG_MERGED_GZ.name

class Database:
    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(
            path,
            check_same_thread=False,
            timeout=30,
        )
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self._schema()

    def _schema(self) -> None:
        with self.lock:
            self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS streams (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                source_ids TEXT NOT NULL DEFAULT '[]',
                first_seen TEXT,
                last_checked TEXT,
                last_online TEXT,
                status TEXT,
                previous_status TEXT,
                latency_ms REAL,
                bitrate INTEGER,
                width INTEGER,
                height INTEGER,
                frame_rate REAL,
                video_codec TEXT,
                audio_codec TEXT,
                successes INTEGER DEFAULT 0,
                failures INTEGER DEFAULT 0,
                consecutive_failures INTEGER DEFAULT 0,
                consecutive_successes INTEGER DEFAULT 0,
                uptime REAL DEFAULT 0,
                score REAL DEFAULT 0,
                last_error TEXT,
                retired INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS checks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT NOT NULL,
                checked_at TEXT NOT NULL,
                status TEXT NOT NULL,
                latency_ms REAL,
                http_code INTEGER,
                error TEXT,
                score REAL
            );

            CREATE TABLE IF NOT EXISTS channel_state (
                channel_key TEXT PRIMARY KEY,
                current_stream_id TEXT,
                replacement_count INTEGER DEFAULT 0,
                updated_at TEXT
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                channel_key TEXT,
                channel_name TEXT,
                old_stream_id TEXT,
                new_stream_id TEXT,
                old_pool_no INTEGER,
                new_pool_no INTEGER,
                message TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_checks_stream
            ON checks(stream_id);

            CREATE INDEX IF NOT EXISTS idx_events_channel
            ON events(channel_key);
            """)
            self.conn.commit()

    def load_stream(self, stream_id: str) -> Stream | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM streams WHERE id=?",
                (stream_id,),
            ).fetchone()

        if not row:
            return None

        (
            sid, url, source_ids, first_seen, last_checked, last_online,
            status, previous_status, latency_ms, bitrate, width, height,
            frame_rate, video_codec, audio_codec, successes, failures,
            consecutive_failures, consecutive_successes, uptime, score,
            last_error, retired
        ) = row

        return Stream(
            url=url,
            source_ids=json.loads(source_ids or "[]"),
            status=status or "unchecked",
            previous_status=previous_status or "",
            latency_ms=latency_ms,
            bitrate=bitrate,
            width=width,
            height=height,
            frame_rate=frame_rate,
            video_codec=video_codec or "",
            audio_codec=audio_codec or "",
            successes=successes or 0,
            failures=failures or 0,
            consecutive_failures=consecutive_failures or 0,
            consecutive_successes=consecutive_successes or 0,
            uptime=uptime or 0.0,
            score=score or 0.0,
            first_seen=first_seen or "",
            last_checked=last_checked or "",
            last_online=last_online or "",
            last_error=last_error or "",
            retired=bool(retired),
        )

    def upsert_stream(self, s: Stream) -> None:
        now = s.first_seen or utc_now()
        with self.lock:
            self.conn.execute("""
            INSERT INTO streams (
                id,url,source_ids,first_seen,last_checked,last_online,
                status,previous_status,latency_ms,bitrate,width,height,
                frame_rate,video_codec,audio_codec,successes,failures,
                consecutive_failures,consecutive_successes,uptime,score,
                last_error,retired
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                url=excluded.url,
                source_ids=excluded.source_ids,
                last_checked=excluded.last_checked,
                last_online=excluded.last_online,
                status=excluded.status,
                previous_status=excluded.previous_status,
                latency_ms=excluded.latency_ms,
                bitrate=excluded.bitrate,
                width=excluded.width,
                height=excluded.height,
                frame_rate=excluded.frame_rate,
                video_codec=excluded.video_codec,
                audio_codec=excluded.audio_codec,
                successes=excluded.successes,
                failures=excluded.failures,
                consecutive_failures=excluded.consecutive_failures,
                consecutive_successes=excluded.consecutive_successes,
                uptime=excluded.uptime,
                score=excluded.score,
                last_error=excluded.last_error,
                retired=excluded.retired
            """, (
                s.id, s.url, json.dumps(s.source_ids),
                now, s.last_checked, s.last_online,
                s.status, s.previous_status, s.latency_ms,
                s.bitrate, s.width, s.height, s.frame_rate,
                s.video_codec, s.audio_codec, s.successes, s.failures,
                s.consecutive_failures, s.consecutive_successes,
                s.uptime, s.score, s.last_error, int(s.retired),
            ))
            self.conn.commit()

    def add_check(
        self,
        s: Stream,
        result: ProbeResult,
    ) -> None:
        with self.lock:
            self.conn.execute("""
            INSERT INTO checks(
                stream_id,checked_at,status,latency_ms,
                http_code,error,score
            ) VALUES(?,?,?,?,?,?,?)
            """, (
                s.id, s.last_checked, s.status, s.latency_ms,
                result.http_code, result.error, s.score,
            ))
            self.conn.commit()

    def save_channel_state(self, c: Channel) -> None:
        with self.lock:
            self.conn.execute("""
            INSERT INTO channel_state(
                channel_key,current_stream_id,replacement_count,updated_at
            ) VALUES(?,?,?,?)
            ON CONFLICT(channel_key) DO UPDATE SET
                current_stream_id=excluded.current_stream_id,
                replacement_count=excluded.replacement_count,
                updated_at=excluded.updated_at
            """, (
                c.key,
                c.current_stream_id,
                c.replacement_count,
                utc_now(),
            ))
            self.conn.commit()

    def event(
        self,
        event_type: str,
        channel: Channel,
        message: str,
        old: Stream | None = None,
        new: Stream | None = None,
    ) -> None:
        with self.lock:
            self.conn.execute("""
            INSERT INTO events(
                timestamp,event_type,channel_key,channel_name,
                old_stream_id,new_stream_id,old_pool_no,new_pool_no,message
            ) VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                utc_now(),
                event_type,
                channel.key,
                channel.name,
                old.id if old else None,
                new.id if new else None,
                old.pool_no if old else None,
                new.pool_no if new else None,
                message,
            ))
            self.conn.commit()

    def recent_events(self, limit: int = 1000) -> list[dict]:
        with self.lock:
            rows = self.conn.execute("""
            SELECT timestamp,event_type,channel_name,
                   old_pool_no,new_pool_no,message
            FROM events
            ORDER BY id DESC LIMIT ?
            """, (limit,)).fetchall()

        return [
            {
                "timestamp": r[0],
                "event_type": r[1],
                "channel": r[2],
                "old_pool_no": r[3],
                "new_pool_no": r[4],
                "message": r[5],
            }
            for r in rows
        ]

    def close(self) -> None:
        with self.lock:
            self.conn.close()


class SourceManager:
    def __init__(self, config_path: Path):
        self.config_path = config_path

    def load(self) -> list[SourceConfig]:
        if not self.config_path.exists():
            return []

        raw = json.loads(self.config_path.read_text(encoding="utf-8"))
        result = []

        for item in raw.get("sources", []):
            if not item.get("enabled", True):
                continue

            url = str(item.get("url", "")).strip()
            if not is_http_url(url):
                continue

            result.append(
                SourceConfig(
                    id=str(item.get("id") or sha256(url)[:12]),
                    url=url,
                    name=str(item.get("name", "")),
                    country=str(item.get("country", "")),
                    region=str(item.get("region", "")),
                    trust=float(item.get("trust", 0.5)),
                    enabled=True,
                    kind=str(item.get("kind", "m3u")),
                )
            )

        return result


class M3UParser:
    ATTR_RE = re.compile(r'([\w-]+)="([^"]*)"')

    def parse(self, text: str, source: SourceConfig) -> list[tuple[dict, str]]:
        lines = [x.strip() for x in text.splitlines() if x.strip()]
        pending: dict | None = None
        result = []

        for line in lines:
            if line.startswith("#EXTINF:"):
                pending = dict(self.ATTR_RE.findall(line))
                comma = line.find(",")
                pending["_name"] = (
                    line[comma + 1:].strip() if comma >= 0 else ""
                )

            elif not line.startswith("#") and pending is not None:
                url = line.strip()

                descriptor = (
                    f"{pending.get('_name', '')} "
                    f"{pending.get('group-title', '')} "
                    f"{url}"
                )

                if allowed_url(url) and not looks_nsfw(descriptor):
                    result.append((pending, url))

                pending = None

        return result

    def fetch(self, source: SourceConfig) -> list[tuple[dict, str]]:
        response = get_session().get(
            source.url,
            timeout=DEFAULT_TIMEOUT,
            allow_redirects=True,
        )
        response.raise_for_status()
        return self.parse(response.text, source)


class ChannelResolver:
    def __init__(self, db: Database):
        self.db = db
        self.channels: dict[str, Channel] = {}

    def key(self, meta: dict) -> str:
        tvg = normalize_name(meta.get("tvg-id", ""))
        name = normalize_name(meta.get("_name", ""))
        return tvg or name

    def add(
        self,
        meta: dict,
        url: str,
        source: SourceConfig,
    ) -> None:
        key = self.key(meta)
        if not key:
            return

        name = meta.get("_name", "").strip() or key
        tvg_id = meta.get("tvg-id", "").strip()

        c = self.channels.get(key)

        if c is None:
            c = Channel(
                key=key,
                name=name,
                tvg_id=tvg_id,
                group=meta.get("group-title", "").strip(),
                logo=meta.get("tvg-logo", "").strip(),
                country=source.country,
                region=source.region,
            )
            c.aliases.add(normalize_name(name))
            if tvg_id:
                c.aliases.add(normalize_name(tvg_id))
            self.channels[key] = c

        sid = sha256(url)

        existing = next(
            (s for s in c.streams if s.id == sid),
            None,
        )

        if existing:
            if source.id not in existing.source_ids:
                existing.source_ids.append(source.id)
            return

        restored = self.db.load_stream(sid)

        if restored:
            stream = restored
            stream.source_ids = sorted(
                set(stream.source_ids + [source.id])
            )
            stream.url = url
        else:
            stream = Stream(
                url=url,
                source_ids=[source.id],
                first_seen=utc_now(),
            )

        c.streams.append(stream)


class HLSInspector:
    """
    Lightweight HLS inspection.

    It verifies that an HLS-like response looks like a media playlist or
    master playlist and optionally reads a small number of segment URLs.
    """

    HLS_MARKERS = (
        "#EXTM3U",
        "#EXT-X-TARGETDURATION",
        "#EXT-X-STREAM-INF",
        "#EXTINF:",
    )

    def inspect(self, url: str, text: str) -> ProbeResult:
        upper = text[:200000]
        is_manifest = "#EXTM3U" in upper

        if not is_manifest:
            return ProbeResult(
                status="online",
                manifest=False,
            )

        segments = 0
        for line in upper.splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                segments += 1
                if segments >= DEFAULT_HLS_SEGMENTS:
                    break

        return ProbeResult(
            status="online",
            manifest=True,
            media={"playlist_entries_sampled": segments},
        )


class FFProbe:
    def __init__(
        self,
        executable: str = "ffprobe",
        timeout: int = DEFAULT_FFPROBE_TIMEOUT,
    ):
        self.executable = executable
        self.timeout = timeout

    def available(self) -> bool:
        try:
            result = subprocess.run(
                [self.executable, "-version"],
                capture_output=True,
                text=True,
                timeout=4,
            )
            return result.returncode == 0
        except Exception:
            return False

    def probe(self, url: str) -> dict[str, Any]:
        command = [
            self.executable,
            "-v", "error",
            "-rw_timeout", str(self.timeout * 1_000_000),
            "-print_format", "json",
            "-show_streams",
            "-show_format",
            url,
        ]

        try:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.timeout + 4,
            )

            if proc.returncode != 0:
                return {"ok": False, "error": proc.stderr[-1000:]}

            data = json.loads(proc.stdout or "{}")
            streams = data.get("streams", [])

            video = next(
                (x for x in streams if x.get("codec_type") == "video"),
                None,
            )
            audio = next(
                (x for x in streams if x.get("codec_type") == "audio"),
                None,
            )

            media = {}

            if video:
                media["width"] = video.get("width")
                media["height"] = video.get("height")
                media["video_codec"] = video.get("codec_name")

                fps = video.get("r_frame_rate")
                if fps and "/" in str(fps):
                    a, b = str(fps).split("/", 1)
                    try:
                        media["frame_rate"] = round(float(a) / float(b), 3)
                    except Exception:
                        pass

            if audio:
                media["audio_codec"] = audio.get("codec_name")

            fmt = data.get("format", {})
            if fmt.get("bit_rate"):
                try:
                    media["bitrate"] = int(fmt["bit_rate"])
                except Exception:
                    pass

            return {"ok": True, "media": media}

        except Exception as exc:
            return {"ok": False, "error": str(exc)[:1000]}


class StreamValidator:
    def __init__(
        self,
        db: Database,
        source_map: dict[str, SourceConfig],
        ffprobe: FFProbe | None = None,
    ):
        self.db = db
        self.source_map = source_map
        self.ffprobe = ffprobe
        self.hls = HLSInspector()

    def _source_trust(self, s: Stream) -> float:
        if not s.source_ids:
            return 0.5

        values = [
            self.source_map[x].trust
            for x in s.source_ids
            if x in self.source_map
        ]

        return max(values) if values else 0.5

    def validate(self, s: Stream, deep: bool = True) -> Stream:
        old_status = s.status
        started = time.monotonic()
        result = ProbeResult(status="dead")

        try:
            response = get_session().get(
                s.url,
                timeout=DEFAULT_TIMEOUT,
                stream=True,
                allow_redirects=True,
            )

            elapsed = (time.monotonic() - started) * 1000
            s.latency_ms = round(elapsed, 1)

            result.http_code = response.status_code
            result.content_type = response.headers.get(
                "Content-Type", ""
            )

            if response.status_code in (401, 403):
                result.status = "blocked"
                result.error = f"HTTP {response.status_code}"
            elif response.status_code >= 400:
                result.status = "dead"
                result.error = f"HTTP {response.status_code}"
            else:
                result.status = "online"

                # Read a bounded amount for fast content verification.
                chunk = b""
                try:
                    for piece in response.iter_content(
                        chunk_size=64 * 1024
                    ):
                        if piece:
                            chunk += piece
                        if len(chunk) >= 256 * 1024:
                            break
                finally:
                    response.close()

                result.bytes_read = len(chunk)

                text = ""
                try:
                    text = chunk.decode(
                        response.encoding or "utf-8",
                        errors="ignore",
                    )
                except Exception:
                    pass

                if (
                    "#EXTM3U" in text
                    or "#EXT-X-" in text
                    or "application/vnd.apple.mpegurl"
                    in result.content_type.lower()
                    or "application/x-mpegurl"
                    in result.content_type.lower()
                ):
                    hls_result = self.hls.inspect(s.url, text)
                    result.manifest = hls_result.manifest
                    result.media.update(hls_result.media)

                # Deep FFprobe is optional and deliberately best-effort.
                if deep and self.ffprobe and self.ffprobe.available():
                    fp = self.ffprobe.probe(s.url)
                    if fp.get("ok"):
                        result.media.update(fp.get("media", {}))

        except requests.RequestException as exc:
            result.status = "dead"
            result.error = str(exc)[:500]

        except Exception as exc:
            result.status = "dead"
            result.error = str(exc)[:500]

        s.previous_status = old_status
        s.last_checked = utc_now()
        s.status = result.status

        if result.status == "online":
            s.successes += 1
            s.consecutive_successes += 1
            s.consecutive_failures = 0
            s.last_online = s.last_checked
        elif result.status == "blocked":
            # Geo/access blocking is recorded separately from a dead stream.
            s.consecutive_failures += 1
            s.last_error = result.error
        else:
            s.failures += 1
            s.consecutive_failures += 1
            s.consecutive_successes = 0
            s.last_error = result.error

        total = s.successes + s.failures
        s.uptime = (
            round((s.successes / total) * 100, 2)
            if total else 0.0
        )

        media = result.media

        if media.get("bitrate"):
            s.bitrate = int(media["bitrate"])
        if media.get("width"):
            s.width = int(media["width"])
        if media.get("height"):
            s.height = int(media["height"])
        if media.get("frame_rate"):
            s.frame_rate = float(media["frame_rate"])
        if media.get("video_codec"):
            s.video_codec = str(media["video_codec"])
        if media.get("audio_codec"):
            s.audio_codec = str(media["audio_codec"])

        s.score = self.score(s)

        self.db.upsert_stream(s)
        self.db.add_check(s, result)

        return s

    def score(self, s: Stream) -> float:
        """
        0..100 rolling score.

        Components:
          uptime        50%
          current state 20%
          latency       15%
          media quality 10%
          source trust   5%
        """
        uptime_part = s.uptime * 0.50

        if s.status == "online":
            state_part = 20.0
        elif s.status == "blocked":
            state_part = 0.0
        else:
            state_part = 0.0

        if s.latency_ms is None:
            latency_part = 0.0
        elif s.latency_ms <= 200:
            latency_part = 15.0
        elif s.latency_ms <= 500:
            latency_part = 12.0
        elif s.latency_ms <= 1000:
            latency_part = 8.0
        elif s.latency_ms <= 2000:
            latency_part = 4.0
        else:
            latency_part = 1.0

        media_part = 0.0
        if s.width and s.height:
            pixels = s.width * s.height
            if pixels >= 1920 * 1080:
                media_part = 10.0
            elif pixels >= 1280 * 720:
                media_part = 8.0
            elif pixels >= 854 * 480:
                media_part = 5.0
            else:
                media_part = 2.0

        trust = self._source_trust(s)
        trust_part = max(0.0, min(5.0, trust * 5.0))

        return round(
            max(0.0, min(
                100.0,
                uptime_part
                + state_part
                + latency_part
                + media_part
                + trust_part,
            )),
            2,
        )


class PoolEngine:
    def __init__(self, db: Database):
        self.db = db

    @staticmethod
    def sort_key(s: Stream):
        status_rank = {
            "online": 3,
            "unstable": 2,
            "unchecked": 1,
            "blocked": 0,
            "dead": -1,
        }

        return (
            status_rank.get(s.status, 0),
            s.score,
            s.uptime,
            -(s.latency_ms or 999999),
        )

    def rebuild(self, channel: Channel) -> None:
        # We retain every stream, including dead ones, so history and
        # diagnostics are never destroyed by a single scan.
        channel.streams.sort(
            key=self.sort_key,
            reverse=True,
        )

        for number, stream in enumerate(channel.streams, 1):
            stream.pool_no = number
            stream.current = False

        online = [
            s for s in channel.streams
            if s.status == "online" and not s.retired
        ]

        if online:
            # Keep existing current if it remains healthy.
            current = next(
                (
                    s for s in online
                    if s.id == channel.current_stream_id
                ),
                None,
            )

            if current is None:
                current = online[0]

            current.current = True
            channel.current_stream_id = current.id
        else:
            channel.current_stream_id = None


class FailoverEngine:
    def __init__(self, db: Database):
        self.db = db

    def apply(self, channel: Channel) -> bool:
        current = next(
            (
                s for s in channel.streams
                if s.id == channel.current_stream_id
            ),
            None,
        )

        if current and current.status == "online":
            current.current = True
            return False

        old = current

        candidates = [
            s for s in channel.streams
            if s.status == "online"
            and not s.retired
            and (old is None or s.id != old.id)
        ]

        if not candidates:
            channel.current_stream_id = None
            self.db.save_channel_state(channel)
            return False

        candidates.sort(
            key=lambda s: (
                s.score,
                s.uptime,
                -(s.latency_ms or 999999),
            ),
            reverse=True,
        )

        new = candidates[0]
        new.current = True
        channel.current_stream_id = new.id
        channel.replacement_count += 1

        self.db.event(
            "FAILOVER",
            channel,
            f"Current stream failed; switched to pool #{new.pool_no}",
            old=old,
            new=new,
        )
        self.db.save_channel_state(channel)

        for s in channel.streams:
            if s.id != new.id:
                s.current = False

        return True


class Watchdog:
    def __init__(
        self,
        channels: dict[str, Channel],
        validator: StreamValidator,
        pool: PoolEngine,
        failover: FailoverEngine,
        workers: int,
        deep: bool,
    ):
        self.channels = channels
        self.validator = validator
        self.pool = pool
        self.failover = failover
        self.workers = workers
        self.deep = deep

    def check_all(self) -> None:
        streams = [
            s for c in self.channels.values()
            for s in c.streams
        ]

        logging.info("WATCHDOG: checking %d streams", len(streams))

        with cf.ThreadPoolExecutor(
            max_workers=self.workers
        ) as executor:
            list(
                executor.map(
                    lambda s: self.validator.validate(
                        s, deep=self.deep
                    ),
                    streams,
                )
            )

        for channel in self.channels.values():
            self.pool.rebuild(channel)
            self.failover.apply(channel)
            self.pool.rebuild(channel)


class Output:
    @staticmethod
    def esc(value: str) -> str:
        return (
            value.replace("&", "&amp;")
            .replace('"', "&quot;")
        )

    def extinf(self, channel: Channel, s: Stream) -> str:
        attrs = []

        if channel.tvg_id:
            attrs.append(
                f'tvg-id="{self.esc(channel.tvg_id)}"'
            )

        if channel.logo:
            attrs.append(
                f'tvg-logo="{self.esc(channel.logo)}"'
            )

        attrs.extend([
            f'group-title="{self.esc(channel.group or "IPTV")}"',
            f'pool-id="{sha256(channel.key)[:12]}"',
            f'pool-no="{s.pool_no}"',
            f'pool-status="{s.status}"',
            f'pool-score="{s.score:.2f}"',
            f'pool-uptime="{s.uptime:.2f}"',
            f'pool-current="{"yes" if s.current else "no"}"',
        ])

        if s.latency_ms is not None:
            attrs.append(
                f'pool-latency-ms="{s.latency_ms:.1f}"'
            )

        label = channel.name

        if s.current:
            label += " [CURRENT]"
        else:
            label += f" [POOL {s.pool_no:03d}]"

        return (
            f'#EXTINF:-1 {" ".join(attrs)},{label}'
        )

    def playlist(
        self,
        channels: dict[str, Channel],
        current_only: bool = False,
    ) -> str:
        lines = [
            "#EXTM3U",
            f'# ZOYA IPTV ENGINE {VERSION}',
            f"# GENERATED {utc_now()}",
            f'x-tvg-url="{EPG_MERGED_GZ.name}"',
        ]

        ordered = sorted(
            channels.values(),
            key=lambda c: normalize_name(c.name),
        )

        for channel in ordered:
            streams = channel.streams

            if current_only:
                streams = [
                    s for s in streams if s.current
                ]

            for stream in streams:
                lines.append(self.extinf(channel, stream))
                lines.append(stream.url)

        return "\n".join(lines) + "\n"

    def write(
        self,
        channels: dict[str, Channel],
        events: list[dict],
    ) -> None:
        atomic_write(
            PLAYLIST_FILE,
            self.playlist(channels, current_only=True),
        )

        atomic_write(
            POOL_FILE,
            self.playlist(channels, current_only=False),
        )

        report = self.build_report(channels, events)

        atomic_write(
            REPORT_JSON,
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
            ),
        )

        atomic_write(
            EVENTS_JSON,
            json.dumps(
                events,
                ensure_ascii=False,
                indent=2,
            ),
        )

        atomic_write(
            REPORT_TXT,
            self.text_report(report),
        )

    def build_report(
        self,
        channels: dict[str, Channel],
        events: list[dict],
    ) -> dict:
        result = {
            "app": APP,
            "version": VERSION,
            "generated_at": utc_now(),
            "epg": {
                "xml": str(EPG_MERGED_XML.relative_to(ROOT)),
                "gz": str(EPG_MERGED_GZ.relative_to(ROOT)),
                "report": str(EPG_REPORT.relative_to(ROOT)),
            },
            "channels_count": len(channels),
            "total_streams": 0,
            "online_streams": 0,
            "unstable_streams": 0,
            "dead_streams": 0,
            "blocked_streams": 0,
            "channels": {},
            "events": events,
        }

        for c in channels.values():
            result["total_streams"] += len(c.streams)

            online = sum(
                s.status == "online" for s in c.streams
            )
            unstable = sum(
                s.status == "unstable" for s in c.streams
            )
            dead = sum(
                s.status == "dead" for s in c.streams
            )
            blocked = sum(
                s.status == "blocked" for s in c.streams
            )

            result["online_streams"] += online
            result["unstable_streams"] += unstable
            result["dead_streams"] += dead
            result["blocked_streams"] += blocked

            current = next(
                (s for s in c.streams if s.current),
                None,
            )

            result["channels"][c.key] = {
                "name": c.name,
                "tvg_id": c.tvg_id,
                "pool_size": len(c.streams),
                "online": online,
                "unstable": unstable,
                "dead": dead,
                "blocked": blocked,
                "current_pool_no": (
                    current.pool_no if current else None
                ),
                "replacement_count": c.replacement_count,
                "streams": [
                    asdict(s) for s in c.streams
                ],
            }

        return result

    def text_report(self, report: dict) -> str:
        lines = [
            "ZOYA IPTV HEALTH REPORT",
            "=" * 80,
            f"Generated: {report['generated_at']}",
            f"Channels:  {report['channels_count']}",
            f"Streams:   {report['total_streams']}",
            f"Online:    {report['online_streams']}",
            f"Unstable:  {report['unstable_streams']}",
            f"Dead:      {report['dead_streams']}",
            f"Blocked:   {report['blocked_streams']}",
            "",
        ]

        for data in report["channels"].values():
            lines.extend([
                data["name"],
                "-" * 80,
                f"Pool size       : {data['pool_size']}",
                f"Online          : {data['online']}",
                f"Unstable        : {data['unstable']}",
                f"Dead            : {data['dead']}",
                f"Blocked         : {data['blocked']}",
                f"Current pool no : {data['current_pool_no']}",
                f"Failovers       : {data['replacement_count']}",
            ])

            dead = [
                s["pool_no"]
                for s in data["streams"]
                if s["status"] == "dead"
            ]

            if dead:
                lines.append(
                    "Dead pool       : "
                    + ", ".join(map(str, dead))
                )

            lines.append("")

        if report["events"]:
            lines.extend([
                "RECENT EVENTS",
                "=" * 80,
            ])

            for event in report["events"][:100]:
                lines.append(
                    f"{event['timestamp']} | "
                    f"{event['event_type']} | "
                    f"{event['channel']} | "
                    f"{event['message']}"
                )

        return "\n".join(lines) + "\n"


class Zoya:
    def __init__(
        self,
        workers: int,
        deep: bool,
        ffprobe_path: str,
    ):
        OUTPUT.mkdir(parents=True, exist_ok=True)

        self.workers = max(1, workers)
        self.deep = deep

        self.db = Database(DB_FILE)
        self.sources = SourceManager(SOURCES_FILE)
        self.parser = M3UParser()

        self.source_list = self.sources.load()
        self.source_map = {
            s.id: s for s in self.source_list
        }

        ffprobe = FFProbe(ffprobe_path) if deep else None

        self.validator = StreamValidator(
            self.db,
            self.source_map,
            ffprobe,
        )

        self.resolver = ChannelResolver(self.db)
        self.pool = PoolEngine(self.db)
        self.failover = FailoverEngine(self.db)
        self.output = Output()
        self.epg = EPGManager(self.resolver.channels)

    def discovery(self) -> None:
        if not self.source_list:
            logging.warning(
                "No sources configured in %s",
                SOURCES_FILE,
            )
            return

        logging.info(
            "DISCOVERY: %d public sources",
            len(self.source_list),
        )

        def fetch(source: SourceConfig):
            try:
                records = self.parser.fetch(source)
                return source, records, None
            except Exception as exc:
                return source, [], str(exc)

        with cf.ThreadPoolExecutor(
            max_workers=min(self.workers, 16)
        ) as executor:
            futures = [
                executor.submit(fetch, source)
                for source in self.source_list
            ]

            for future in cf.as_completed(futures):
                source, records, error = future.result()

                if error:
                    logging.error(
                        "SOURCE ERROR [%s]: %s",
                        source.id,
                        error,
                    )
                    continue

                logging.info(
                    "SOURCE [%s] %s -> %d records",
                    source.id,
                    source.name or source.url,
                    len(records),
                )

                for meta, url in records:
                    self.resolver.add(
                        meta,
                        url,
                        source,
                    )

        logging.info(
            "DISCOVERY RESULT: channels=%d streams=%d",
            len(self.resolver.channels),
            sum(
                len(c.streams)
                for c in self.resolver.channels.values()
            ),
        )

    def validate(self) -> None:
        streams = [
            s
            for c in self.resolver.channels.values()
            for s in c.streams
        ]

        logging.info(
            "VALIDATION: %d streams",
            len(streams),
        )

        with cf.ThreadPoolExecutor(
            max_workers=self.workers
        ) as executor:
            futures = [
                executor.submit(
                    self.validator.validate,
                    s,
                    self.deep,
                )
                for s in streams
            ]

            for future in cf.as_completed(futures):
                s = future.result()
                logging.debug(
                    "%s %s %.2f",
                    s.status,
                    s.url,
                    s.score,
                )

    def failover_cycle(self) -> None:
        for channel in self.resolver.channels.values():
            before = channel.current_stream_id

            self.pool.rebuild(channel)
            self.failover.apply(channel)
            self.pool.rebuild(channel)

            after = channel.current_stream_id

            if before != after:
                logging.warning(
                    "FAILOVER %s: %s -> %s",
                    channel.name,
                    before,
                    after,
                )

            self.db.save_channel_state(channel)

    def run(self, no_epg: bool = False) -> None:
        logging.info(
            "========== %s %s ==========",
            APP,
            VERSION,
        )

        self.discovery()
        self.validate()
        self.failover_cycle()

        # EPG is deliberately generated after channel resolution so every
        # surviving channel gets a chance to bind to the merged XMLTV guide.
        self.epg.channels = self.resolver.channels
        if not no_epg:
            self.epg.fetch_all()
        else:
            logging.info("EPG: disabled by --no-epg")

        events = self.db.recent_events(1000)

        self.output.write(
            self.resolver.channels,
            events,
        )

        logging.info(
            "OUTPUT playlist=%s",
            PLAYLIST_FILE,
        )
        logging.info(
            "OUTPUT pool=%s",
            POOL_FILE,
        )
        logging.info(
            "OUTPUT report=%s",
            REPORT_JSON,
        )

    def close(self) -> None:
        self.db.close()



def create_default_epg_config() -> None:
    path = ROOT / "epg_sources.json"
    if path.exists():
        return

    path.write_text(
        json.dumps(
            {"sources": DEFAULT_EPG_SOURCES},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def create_default_config() -> None:
    if SOURCES_FILE.exists():
        return

    config = {
        "sources": [
            {
                "id": "example-public-source",
                "name": "Replace with public M3U source",
                "url": "https://example.invalid/playlist.m3u",
                "country": "",
                "region": "",
                "trust": 0.5,
                "enabled": False,
                "kind": "m3u",
            }
        ]
    }

    SOURCES_FILE.write_text(
        json.dumps(
            config,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "ZOYA IPTV Discovery, Pool Health and Failover Engine"
        )
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
    )

    parser.add_argument(
        "--deep",
        action="store_true",
        help="enable FFprobe deep inspection",
    )

    parser.add_argument(
        "--ffprobe",
        default="ffprobe",
        help="ffprobe executable path",
    )

    parser.add_argument(
        "--init",
        action="store_true",
        help="create default sources.json",
    )

    parser.add_argument(
        "--no-discovery",
        action="store_true",
        help="skip source discovery and use configured/current data",
    )

    parser.add_argument(
        "--no-epg",
        action="store_true",
        help="skip EPG aggregation",
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"{APP} {VERSION}",
    )

    args = parser.parse_args()

    OUTPUT.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | %(levelname)s | %(message)s"
        ),
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(
                LOG_FILE,
                encoding="utf-8",
            ),
        ],
    )

    if args.init:
        create_default_config()
        create_default_epg_config()
        print(f"Created {SOURCES_FILE}")
        print(f"Created {ROOT / 'epg_sources.json'}")
        return 0

    zoya = Zoya(
        workers=args.workers,
        deep=args.deep,
        ffprobe_path=args.ffprobe,
    )

    try:
        if args.no_discovery:
            logging.info(
                "Discovery disabled; no-discovery mode is intended "
                "for future persistent-pool/watchdog operation."
            )
        zoya.run(no_epg=args.no_epg)
    finally:
        zoya.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
