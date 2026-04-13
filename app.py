from __future__ import annotations

import ipaddress
import logging
import mimetypes
import os
import re
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from flask import Flask, Response, jsonify, render_template, request, send_file
from werkzeug.exceptions import BadRequest
from yt_dlp import YoutubeDL
from yt_dlp.utils import DownloadError

APP_ROOT = Path(__file__).resolve().parent
TMP_ROOT = APP_ROOT / "runtime_downloads"
JOBS_ROOT = TMP_ROOT / "jobs"
TMP_ROOT.mkdir(parents=True, exist_ok=True)
JOBS_ROOT.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("reclip")

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["JSON_AS_ASCII"] = False
app.config["MAX_CONTENT_LENGTH"] = 64 * 1024

MP3_BITRATES = [320, 192, 128]
SAFE_NAME_RE = re.compile(r"[^\w\-.() ]+", re.UNICODE)
SAFE_JOB_ID_RE = re.compile(r"^[a-f0-9]{32}$")
JOB_RETENTION_SECONDS = int(os.getenv("JOB_RETENTION_SECONDS", str(12 * 3600)))
INFO_CACHE_TTL_SECONDS = int(os.getenv("INFO_CACHE_TTL_SECONDS", "600"))
INFO_CACHE_MAX_SIZE = int(os.getenv("INFO_CACHE_MAX_SIZE", "256"))

JOB_LOCK = threading.Lock()
JOBS: dict[str, "JobState"] = {}
INFO_CACHE_LOCK = threading.Lock()
INFO_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
PURGE_INTERVAL_SECONDS = int(os.getenv("PURGE_INTERVAL_SECONDS", "300"))
MAX_CONCURRENT_JOBS = int(os.getenv("MAX_CONCURRENT_JOBS", "50"))
MAX_THUMB_BYTES = 5 * 1024 * 1024


class JobCancelled(Exception):
    pass


@dataclass
class JobState:
    job_id: str
    url: str
    requested_format: str
    format_id: str
    requested_title: str
    start_sec: float | None
    end_sec: float | None
    workdir: Path
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    status: str = "queued"
    message: str = "В очереди"
    error: str = ""
    file_path: Path | None = None
    download_name: str = ""
    downloaded_bytes: int | None = None
    total_bytes: int | None = None
    cancel_requested: bool = False
    fetched: bool = False


def touch_job(job: JobState) -> None:
    job.updated_at = time.time()


def set_job_state(job: JobState, *, status: str | None = None, message: str | None = None, error: str | None = None) -> None:
    with JOB_LOCK:
        if status is not None:
            job.status = status
        if message is not None:
            job.message = message
        if error is not None:
            job.error = error
        touch_job(job)


def serialize_job(job: JobState) -> dict[str, Any]:
    percent = None
    if job.total_bytes and job.total_bytes > 0 and job.downloaded_bytes is not None:
        percent = max(0, min(100, int(round((job.downloaded_bytes / job.total_bytes) * 100))))

    return {
        "job_id": job.job_id,
        "status": job.status,
        "message": job.message,
        "error": job.error,
        "downloaded_bytes": job.downloaded_bytes,
        "total_bytes": job.total_bytes,
        "percent": percent,
        "cancel_requested": job.cancel_requested,
        "ready": job.status == "ready" and job.file_path is not None and job.file_path.exists(),
    }


def purge_expired_jobs() -> None:
    now = time.time()
    to_remove: list[str] = []
    with JOB_LOCK:
        for job_id, job in JOBS.items():
            if job.status in {"ready", "error", "cancelled"} and now - job.updated_at > JOB_RETENTION_SECONDS:
                to_remove.append(job_id)
    for job_id in to_remove:
        remove_job(job_id)


def remove_job(job_id: str) -> None:
    with JOB_LOCK:
        job = JOBS.pop(job_id, None)
    if job:
        cleanup_tree(job.workdir)


def require_ffmpeg() -> str:
    ffmpeg_path = shutil.which("ffmpeg")
    if not ffmpeg_path:
        raise RuntimeError("ffmpeg не найден в контейнере")
    return ffmpeg_path


def get_cached_video_info(url: str) -> dict[str, Any] | None:
    if INFO_CACHE_TTL_SECONDS <= 0:
        return None

    now = time.time()
    with INFO_CACHE_LOCK:
        entry = INFO_CACHE.get(url)
        if not entry:
            return None

        cached_at, info = entry
        if now - cached_at > INFO_CACHE_TTL_SECONDS:
            INFO_CACHE.pop(url, None)
            return None
        return info


def put_cached_video_info(url: str, info: dict[str, Any]) -> None:
    if INFO_CACHE_TTL_SECONDS <= 0 or INFO_CACHE_MAX_SIZE <= 0:
        return

    now = time.time()
    with INFO_CACHE_LOCK:
        INFO_CACHE[url] = (now, info)
        overflow = len(INFO_CACHE) - INFO_CACHE_MAX_SIZE
        if overflow > 0:
            stale_urls = sorted(INFO_CACHE.items(), key=lambda item: item[1][0])[:overflow]
            for stale_url, _ in stale_urls:
                INFO_CACHE.pop(stale_url, None)


def parse_optional_float(raw: str | None) -> float | None:
    if raw is None or raw == "":
        return None
    try:
        value = float(raw)
    except ValueError as exc:
        raise BadRequest("Неверный формат времени") from exc
    if value < 0:
        raise BadRequest("Время не может быть отрицательным")
    return value


def sanitize_filename(name: str, fallback: str = "download") -> str:
    cleaned = SAFE_NAME_RE.sub(" ", name).strip().replace("/", " ").replace("\\", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip(" .")
    return cleaned[:180] or fallback


def format_seconds(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def env_int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = str(os.getenv(name, "")).strip()
    if not raw:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError:
            logger.warning("Invalid integer for %s=%r, using default=%s", name, raw, default)
            value = default

    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def base_ydl_opts() -> dict[str, Any]:
    return {
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "socket_timeout": 20,
        "retries": 2,
        "extractor_retries": 1,
        "fragment_retries": 1,
        "concurrent_fragment_downloads": env_int("YTDLP_CONCURRENT_FRAGMENT_DOWNLOADS", 1, minimum=1, maximum=16),
        "skip_unavailable_fragments": True,
        "restrictfilenames": False,
        "windowsfilenames": False,
        "cachedir": False,
    }


def normalize_source_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    host = parsed.netloc.lower().split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]

    if host == "dzen.ru":
        match = re.fullmatch(r"/shorts/([0-9a-f]{24})/?", parsed.path)
        if match:
            video_id = match.group(1)
            return urllib.parse.urlunparse(("https", "dzen.ru", f"/video/watch/{video_id}", "", "", ""))

    return url


def extract_video_info(url: str) -> dict[str, Any]:
    url = normalize_source_url(url)
    cached_info = get_cached_video_info(url)
    if cached_info is not None:
        return cached_info

    opts = base_ydl_opts() | {"skip_download": True}
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        sanitized = ydl.sanitize_info(info)
        put_cached_video_info(url, sanitized)
        return sanitized


def numeric_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def is_mp4_family(fmt: dict[str, Any]) -> bool:
    ext = (fmt.get("ext") or "").lower()
    container = (fmt.get("container") or "").lower()
    return ext in {"mp4", "m4v", "mov"} or "mp4" in container or "mov" in container


def normalize_codec_name(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw or raw == "none":
        return ""
    return raw.split(".", 1)[0]


def is_h264_codec(value: Any) -> bool:
    codec = normalize_codec_name(value)
    return codec in {"avc1", "avc3", "h264", "x264"}


def is_compatibility_risky_codec(value: Any) -> bool:
    codec = normalize_codec_name(value)
    return codec in {"hev1", "hvc1", "hevc", "h265", "vp9", "vp09", "av01"}


def video_codec_sort_rank(fmt: dict[str, Any]) -> int:
    codec = fmt.get("vcodec")
    if is_h264_codec(codec):
        return 2
    if is_compatibility_risky_codec(codec):
        return 0
    return 1


def find_format_by_id(info: dict[str, Any], format_id: str) -> dict[str, Any] | None:
    for fmt in info.get("formats") or []:
        if str(fmt.get("format_id") or "").strip() == str(format_id).strip():
            return fmt
    return None


def should_transcode_for_compatibility(fmt: dict[str, Any] | None) -> bool:
    if not fmt:
        return False
    return is_compatibility_risky_codec(fmt.get("vcodec"))


def transcode_video_to_h264(
    input_path: Path,
    output_path: Path,
    should_cancel: Callable[[], bool] | None = None,
) -> None:
    ffmpeg_path = require_ffmpeg()
    cmd = [
        ffmpeg_path,
        "-y",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        str(input_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-c:v",
        "libx264",
        "-preset",
        os.getenv("FFMPEG_PRESET", "veryfast"),
        "-crf",
        os.getenv("FFMPEG_CRF", "23"),
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]

    logger.info("Running ffmpeg compatibility transcode")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        while True:
            if should_cancel and should_cancel():
                process.kill()
                raise JobCancelled("cancelled")
            result = process.poll()
            if result is not None:
                break
            time.sleep(0.2)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(stderr.strip() or stdout.strip() or "ffmpeg завершился с ошибкой")
    finally:
        if process.poll() is None:
            process.kill()


def human_size(num_bytes: int | float | None) -> str:
    if not num_bytes:
        return "0 B"
    value = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if value < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{int(num_bytes)} B"


def pick_best_thumbnail(info: dict[str, Any]) -> str:
    thumbnails = info.get("thumbnails") or []
    scored: list[tuple[tuple[float, float, float, int], str]] = []

    for thumb in thumbnails:
        url = str(thumb.get("url") or "").strip()
        if not url:
            continue

        thumb_id = str(thumb.get("id") or "").lower()
        is_sprite = any(marker in thumb_id for marker in ("storyboard", "sprite", "sheet"))
        width = numeric_or_zero(thumb.get("width"))
        height = numeric_or_zero(thumb.get("height"))
        preference = numeric_or_zero(thumb.get("preference"))
        area = width * height
        ext = (thumb.get("ext") or "").lower() or Path(urllib.parse.urlparse(url).path).suffix.lower().lstrip(".")
        ext_bonus = 1 if ext in {"jpg", "jpeg", "png", "webp"} else 0

        score = (
            -1.0 if is_sprite else 0.0,
            preference,
            area,
            ext_bonus,
        )
        scored.append((score, url))

    if scored:
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1]

    return str(info.get("thumbnail") or "").strip()


def proxy_thumbnail_url(src: str) -> str:
    if not src:
        return ""
    return f"/api/thumb?src={urllib.parse.quote(src, safe='')}"


def sort_audio_candidates(formats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        formats,
        key=lambda fmt: (
            numeric_or_zero(fmt.get("quality")),
            numeric_or_zero(fmt.get("abr")),
            numeric_or_zero(fmt.get("tbr")),
            numeric_or_zero(fmt.get("filesize") or fmt.get("filesize_approx")),
        ),
        reverse=True,
    )


def pick_audio_companion(
    video_fmt: dict[str, Any],
    best_audio_any: dict[str, Any] | None,
    best_audio_mp4: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if best_audio_any is None:
        return None

    if is_mp4_family(video_fmt) and best_audio_mp4 is not None:
        return best_audio_mp4
    return best_audio_any


def build_audio_pools(raw_formats: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any] | None, dict[str, Any] | None]:
    audio_only = [
        fmt for fmt in raw_formats
        if fmt.get("vcodec") in {None, "none"} and format_has_own_audio(fmt)
    ]
    if not audio_only:
        return [], None, None

    mp4_audio = [fmt for fmt in audio_only if (fmt.get("ext") or "").lower() in {"m4a", "mp4", "aac"}]
    best_audio_any = sort_audio_candidates(audio_only)[0]
    best_audio_mp4 = sort_audio_candidates(mp4_audio)[0] if mp4_audio else None
    return audio_only, best_audio_any, best_audio_mp4


def estimate_total_filesize(
    video_fmt: dict[str, Any],
    companion_audio: dict[str, Any] | None,
    duration_sec: float | None = None,
) -> tuple[int | None, bool]:
    video_size = video_fmt.get("filesize") or video_fmt.get("filesize_approx")
    video_exact = video_fmt.get("filesize") is not None

    if format_has_own_audio(video_fmt) or companion_audio is None:
        if video_size:
            return (int(video_size), not video_exact)
        if duration_sec and duration_sec > 0:
            tbr = numeric_or_zero(video_fmt.get("tbr"))
            if tbr > 0:
                return int(tbr * 1000 / 8 * duration_sec), True
        return (None, False)

    audio_size = companion_audio.get("filesize") or companion_audio.get("filesize_approx")
    audio_exact = companion_audio.get("filesize") is not None

    def _est_from_tbr(fmt: dict[str, Any], tbr_key: str, fallback_key: str) -> int | None:
        if not duration_sec or duration_sec <= 0:
            return None
        br = numeric_or_zero(fmt.get(tbr_key)) or numeric_or_zero(fmt.get(fallback_key))
        return int(br * 1000 / 8 * duration_sec) if br > 0 else None

    v = int(video_size) if video_size else _est_from_tbr(video_fmt, "tbr", "vbr")
    a = int(audio_size) if audio_size else _est_from_tbr(companion_audio, "tbr", "abr")

    if v and a:
        is_approx = not (video_exact and video_size and audio_exact and audio_size)
        return v + a, is_approx
    if v:
        return v, not (video_exact and video_size)
    if a:
        return a, not (audio_exact and audio_size)
    return None, False


def format_has_own_audio(fmt: dict[str, Any]) -> bool:
    """Check if a format entry itself carries audio, using multiple yt-dlp fields."""
    acodec = fmt.get("acodec")
    if acodec not in {None, "none"}:
        return True
    audio_ext = (fmt.get("audio_ext") or "").lower()
    if audio_ext and audio_ext != "none":
        return True
    audio_channels = fmt.get("audio_channels")
    if audio_channels is not None and audio_channels > 0:
        return True
    abr = numeric_or_zero(fmt.get("abr"))
    if abr > 0:
        return True
    return False


def video_output_has_audio(video_fmt: dict[str, Any], companion_audio: dict[str, Any] | None = None) -> bool:
    if format_has_own_audio(video_fmt):
        return True
    return companion_audio is not None


def build_video_label(
    fmt: dict[str, Any],
    companion_audio: dict[str, Any] | None = None,
    duration_sec: float | None = None,
) -> tuple[str, str]:
    """Return (full_label, dedup_key) where dedup_key is the label without filesize."""
    parts: list[str] = []
    height = fmt.get("height")
    width = fmt.get("width")
    fps = numeric_or_zero(fmt.get("fps"))
    note = (fmt.get("format_note") or "").strip()

    if height:
        parts.append(f"{int(height)}p")
    elif width and fmt.get("resolution"):
        parts.append(str(fmt["resolution"]))
    elif note:
        parts.append(note)
    else:
        parts.append(str(fmt.get("format_id", "format")))

    if fps:
        parts.append(f"{int(fps)} fps")

    dedup_key = " · ".join(parts)

    total_size, is_approx = estimate_total_filesize(fmt, companion_audio, duration_sec)
    if total_size:
        size_label = human_size(total_size)
        parts.append(f"≈ {size_label}" if is_approx else size_label)

    parts.append("со звуком" if video_output_has_audio(fmt, companion_audio) else "без звука")
    dedup_key += " · " + parts[-1]
    return " · ".join(parts), dedup_key


def estimate_mp3_size(duration_sec: int | float | None, bitrate_kbps: int) -> int | None:
    if not duration_sec or duration_sec <= 0:
        return None
    return int(float(duration_sec) * bitrate_kbps * 1000 / 8)


def build_audio_label(fmt: dict[str, Any]) -> str:
    parts: list[str] = []
    abr = numeric_or_zero(fmt.get("abr"))
    ext = (fmt.get("ext") or "audio").upper()
    parts.append(ext)
    if abr:
        parts.append(f"{int(round(abr))} kbps")
    size = fmt.get("filesize") or fmt.get("filesize_approx")
    if size:
        parts.append(human_size(size))
    return " · ".join(parts)


def build_mp3_label(duration_sec: int | float | None, bitrate: int) -> str:
    parts = ["MP3", f"{bitrate} kbps"]
    size = estimate_mp3_size(duration_sec, bitrate)
    if size:
        parts.append(f"≈ {human_size(size)}")
    return " · ".join(parts)


def title_case_site(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    if not value:
        return ""
    tokens = [part for part in value.split() if part]
    compact = "".join(tokens)

    _SITE_EXACT: dict[str, str] = {
        "youtube": "YouTube", "youtu": "YouTube",
        "tiktok": "TikTok", "instagram": "Instagram", "facebook": "Facebook",
        "twitter": "Twitter/X", "twitterx": "Twitter/X", "x": "Twitter/X", "xcom": "Twitter/X",
        "rutube": "Rutube", "reddit": "Reddit", "twitch": "Twitch",
        "pinterest": "Pinterest", "vimeo": "Vimeo",
        "zenyandex": "Дзен", "yandexzen": "Дзен", "dzenru": "Дзен", "dzen": "Дзен", "zen": "Дзен",
        "vk": "VK Видео", "vkontakte": "VK Видео",
        "yandex": "Яндекс", "yandexvideo": "Яндекс",
        "ok": "Одноклассники", "okru": "Одноклассники",
    }

    if compact in _SITE_EXACT:
        return _SITE_EXACT[compact]

    _SITE_CONTAINS = (
        ("youtube", "YouTube"), ("youtu", "YouTube"),
        ("tiktok", "TikTok"), ("instagram", "Instagram"), ("facebook", "Facebook"),
        ("twitterx", "Twitter/X"), ("twitter", "Twitter/X"),
        ("rutube", "Rutube"), ("reddit", "Reddit"), ("twitch", "Twitch"),
        ("pinterest", "Pinterest"), ("vimeo", "Vimeo"),
        ("vkontakte", "VK Видео"), ("dzen", "Дзен"),
        ("yandexvideo", "Яндекс"), ("yandex", "Яндекс"),
        ("okru", "Одноклассники"),
    )
    for key, label in _SITE_CONTAINS:
        if key in compact:
            return label

    for token in tokens:
        if token in _SITE_EXACT:
            return _SITE_EXACT[token]

    return " ".join(part.capitalize() for part in tokens)


def detect_site_name(info: dict[str, Any]) -> str:
    candidates = [
        str(info.get("extractor_key") or ""),
        str(info.get("ie_key") or ""),
        str(info.get("extractor") or ""),
    ]
    for candidate in candidates:
        label = title_case_site(candidate)
        if label:
            return label

    url_candidates = [
        str(info.get("webpage_url") or ""),
        str(info.get("original_url") or ""),
        str(info.get("url") or ""),
    ]
    for raw_url in url_candidates:
        if not raw_url:
            continue
        host = urllib.parse.urlparse(raw_url).netloc.lower()
        host = host.split(":")[0]
        if host.startswith("www."):
            host = host[4:]
        label = title_case_site(host)
        if label:
            return label
        if host:
            parts = host.split(".")
            if len(parts) >= 2:
                return parts[-2].capitalize()
    return ""


def normalize_info_payload(info: dict[str, Any], original_url: str = "") -> dict[str, Any]:
    raw_formats = info.get("formats") or []
    duration_value = numeric_or_zero(info.get("duration"))
    duration = int(duration_value) if duration_value > 0 else None
    audio_only_pool, best_audio_any, best_audio_mp4 = build_audio_pools(raw_formats)

    mp4_formats: list[dict[str, Any]] = []
    seen_video_ids: set[str] = set()
    audio_only_formats: list[dict[str, Any]] = []
    seen_audio_ids: set[str] = set()
    has_any_audio = any(format_has_own_audio(fmt) for fmt in raw_formats)

    for fmt in raw_formats:
        format_id = str(fmt.get("format_id") or "").strip()
        if not format_id:
            continue

        acodec = fmt.get("acodec")
        vcodec = fmt.get("vcodec")
        ext = (fmt.get("ext") or "").lower()

        is_video = vcodec not in {None, "none"}
        is_accepted_container = is_mp4_family(fmt) or ext in {"webm", "3gp"}
        protocol = str(fmt.get("protocol") or "").lower()
        is_hls_dash = any(p in protocol for p in ("m3u8", "http_dash", "dash", "hls"))
        if not is_accepted_container and is_hls_dash and is_video:
            is_accepted_container = True
        has_resolution = fmt.get("height") and int(numeric_or_zero(fmt.get("height"))) > 0

        if is_video and is_accepted_container and has_resolution:
            if format_id not in seen_video_ids:
                seen_video_ids.add(format_id)
                companion_audio = None
                if not format_has_own_audio(fmt):
                    companion_audio = pick_audio_companion(fmt, best_audio_any, best_audio_mp4)
                label, dedup_key = build_video_label(fmt, companion_audio, duration_sec=duration_value if duration_value > 0 else None)
                mp4_formats.append({
                    "id": format_id,
                    "label": label,
                    "_dedup_key": dedup_key,
                    "_sort_height": int(numeric_or_zero(fmt.get("height"))),
                    "_sort_codec_rank": video_codec_sort_rank(fmt),
                    "_sort_fps": int(numeric_or_zero(fmt.get("fps"))),
                    "_sort_tbr": numeric_or_zero(fmt.get("tbr")),
                })
        elif vcodec in {None, "none"} and format_has_own_audio(fmt):
            if format_id not in seen_audio_ids:
                seen_audio_ids.add(format_id)
                audio_only_formats.append({
                    "id": format_id,
                    "label": build_audio_label(fmt),
                    "_sort_abr": numeric_or_zero(fmt.get("abr")),
                })

    mp4_formats.sort(
        key=lambda item: (item["_sort_height"], item["_sort_codec_rank"], item["_sort_fps"], item["_sort_tbr"]),
        reverse=True,
    )
    audio_only_formats.sort(key=lambda item: item["_sort_abr"], reverse=True)

    seen_video_keys: set[str] = set()
    deduped_mp4: list[dict[str, Any]] = []
    for item in mp4_formats:
        key = item["_dedup_key"]
        if key not in seen_video_keys:
            seen_video_keys.add(key)
            deduped_mp4.append(item)
    mp4_formats = deduped_mp4

    seen_audio_labels: set[str] = set()
    deduped_audio: list[dict[str, Any]] = []
    for item in audio_only_formats:
        if item["label"] not in seen_audio_labels:
            seen_audio_labels.add(item["label"])
            deduped_audio.append(item)
    audio_only_formats = deduped_audio

    for item in mp4_formats:
        item.pop("_dedup_key", None)
        item.pop("_sort_height", None)
        item.pop("_sort_codec_rank", None)
        item.pop("_sort_fps", None)
        item.pop("_sort_tbr", None)
    for item in audio_only_formats:
        item.pop("_sort_abr", None)

    mp3_formats = [
        {"id": str(bitrate), "label": build_mp3_label(duration, bitrate)}
        for bitrate in MP3_BITRATES
    ] if has_any_audio else []

    width = numeric_or_zero(info.get("width"))
    height = numeric_or_zero(info.get("height"))
    if not (width and height):
        best_res = max(
            (fmt for fmt in raw_formats if numeric_or_zero(fmt.get("height")) > 0),
            key=lambda f: (numeric_or_zero(f.get("height")), numeric_or_zero(f.get("width"))),
            default=None,
        )
        if best_res:
            width = numeric_or_zero(best_res.get("width"))
            height = numeric_or_zero(best_res.get("height"))

    is_vertical = False
    if height and width:
        is_vertical = height > width
    elif height and not width:
        aspect_ratio = info.get("aspect_ratio")
        if isinstance(aspect_ratio, (int, float)) and aspect_ratio > 0:
            is_vertical = aspect_ratio < 1.0
        else:
            for fmt in raw_formats:
                fw = numeric_or_zero(fmt.get("width"))
                fh = numeric_or_zero(fmt.get("height"))
                if fw and fh:
                    is_vertical = fh > fw
                    break
    if not is_vertical:
        orig_url = original_url or info.get("original_url") or info.get("webpage_url") or ""
        is_vertical = "/shorts/" in orig_url

    thumbnail = proxy_thumbnail_url(pick_best_thumbnail(info))

    return {
        "title": info.get("title") or "",
        "thumbnail": thumbnail,
        "duration": duration,
        "uploader": info.get("uploader") or info.get("channel") or info.get("creator") or "",
        "site": detect_site_name(info),
        "is_vertical": is_vertical,
        "formats": mp4_formats[:12],
        "m4a_formats": audio_only_formats[:8],
        "mp3_formats": mp3_formats,
        "audio_caps": {
            "mp3": bool(mp3_formats),
            "m4a": bool(audio_only_formats),
        },
    }


def choose_video_selector(info: dict[str, Any], format_id: str) -> str:
    for fmt in info.get("formats") or []:
        if str(fmt.get("format_id")) != str(format_id):
            continue
        if fmt.get("vcodec") in {None, "none"}:
            raise BadRequest("Выбран аудиоформат вместо видеоформата")
        if format_has_own_audio(fmt):
            return format_id
        return f"{format_id}+bestaudio[ext=m4a]/{format_id}+bestaudio/{format_id}+best"
    raise BadRequest("Выбранный формат не найден")


def locate_final_media(workdir: Path, preferred_ext: str) -> Path:
    candidates = [
        p for p in workdir.iterdir()
        if p.is_file()
        and not p.name.endswith((".part", ".ytdl", ".temp", ".json", ".jpg", ".jpeg", ".png", ".webp", ".vtt", ".srt"))
    ]
    if not candidates:
        raise FileNotFoundError("Файл после загрузки не найден")

    preferred = [p for p in candidates if p.suffix.lower() == f".{preferred_ext.lower()}".lower()]
    pool = preferred or candidates
    pool.sort(key=lambda p: (p.stat().st_mtime, p.stat().st_size), reverse=True)
    return pool[0]


def trim_media(
    input_path: Path,
    output_path: Path,
    media_kind: str,
    start_sec: float | None,
    end_sec: float | None,
    mp3_bitrate: str = "192",
    should_cancel: Callable[[], bool] | None = None,
) -> None:
    ffmpeg_path = require_ffmpeg()
    cmd = [ffmpeg_path, "-y", "-hide_banner", "-loglevel", "error"]

    if media_kind == "video":
        if start_sec is not None:
            cmd.extend(["-ss", format_seconds(start_sec)])
        cmd.extend(["-i", str(input_path)])
        if end_sec is not None:
            if start_sec is not None:
                cmd.extend(["-t", format_seconds(end_sec - start_sec)])
            else:
                cmd.extend(["-to", format_seconds(end_sec)])
        cmd.extend([
            "-map", "0:v:0",
            "-map", "0:a?",
            "-c:v", "libx264",
            "-preset", os.getenv("FFMPEG_PRESET", "veryfast"),
            "-crf", os.getenv("FFMPEG_CRF", "23"),
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "192k",
            "-avoid_negative_ts", "make_zero",
            "-movflags", "+faststart",
            str(output_path),
        ])
    else:
        cmd.extend(["-i", str(input_path)])
        if start_sec is not None:
            cmd.extend(["-ss", format_seconds(start_sec)])
        if end_sec is not None:
            cmd.extend(["-to", format_seconds(end_sec)])
        cmd.extend([
            "-vn",
            "-codec:a", "libmp3lame",
            "-b:a", f"{mp3_bitrate}k",
            str(output_path),
        ])

    logger.info("Running ffmpeg trim command")
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        while True:
            if should_cancel and should_cancel():
                process.kill()
                process.wait(timeout=5)
                raise JobCancelled("cancelled")
            if process.poll() is not None:
                break
            time.sleep(0.2)
        stdout, stderr = process.communicate()
    finally:
        if process.poll() is None:
            process.kill()

    if process.returncode != 0:
        raise RuntimeError(stderr.strip() or stdout.strip() or "ffmpeg завершился с ошибкой")


def cleanup_tree(path: Path) -> None:
    try:
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
    except Exception:
        logger.exception("Не удалось очистить временную директорию %s", path)


def should_cancel_job(job_id: str) -> bool:
    with JOB_LOCK:
        job = JOBS.get(job_id)
        return bool(job and job.cancel_requested)


def run_prepare_job(job_id: str) -> None:
    with JOB_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return

    def check_cancel() -> None:
        if should_cancel_job(job_id):
            raise JobCancelled("cancelled")

    try:
        set_job_state(job, status="preparing", message="Получение данных о видео...")
        info = extract_video_info(job.url)
        check_cancel()

        safe_title = sanitize_filename(job.requested_title or info.get("title") or "download")
        ydl_opts = base_ydl_opts() | {
            "paths": {"home": str(job.workdir)},
            "outtmpl": {"default": str(job.workdir / "download.%(ext)s")},
            "overwrites": True,
            "prefer_ffmpeg": True,
            "ffmpeg_location": require_ffmpeg(),
        }

        def progress_hook(data: dict[str, Any]) -> None:
            if should_cancel_job(job_id):
                raise JobCancelled("cancelled")

            status = str(data.get("status") or "")
            with JOB_LOCK:
                current = JOBS.get(job_id)
                if not current:
                    return
                if status == "downloading":
                    current.status = "downloading"
                    current.message = "Скачивание файла на сервер..."
                    current.downloaded_bytes = int(numeric_or_zero(data.get("downloaded_bytes"))) or 0
                    total_bytes = data.get("total_bytes") or data.get("total_bytes_estimate")
                    current.total_bytes = int(numeric_or_zero(total_bytes)) or None
                    touch_job(current)
                elif status == "finished":
                    current.status = "processing"
                    current.message = "Сборка и обработка файла..."
                    touch_job(current)

        def postprocessor_hook(data: dict[str, Any]) -> None:
            if should_cancel_job(job_id):
                raise JobCancelled("cancelled")
            label = "Обработка видео..." if job.requested_format == "video" else "Обработка аудио..."
            set_job_state(job, status="processing", message=label)

        ydl_opts["progress_hooks"] = [progress_hook]
        ydl_opts["postprocessor_hooks"] = [postprocessor_hook]

        if job.requested_format == "video":
            if not job.format_id:
                raise BadRequest("Не указан video format_id")
            ydl_opts["format"] = choose_video_selector(info, job.format_id)
            ydl_opts["merge_output_format"] = "mp4"
        else:
            target_bitrate = job.format_id if job.format_id in {str(x) for x in MP3_BITRATES} else "192"
            ydl_opts["format"] = "bestaudio/best"
            ydl_opts["postprocessors"] = [{
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": target_bitrate,
            }]
            ydl_opts["keepvideo"] = False

        with YoutubeDL(ydl_opts) as ydl:
            ydl.download([job.url])

        check_cancel()

        ext = "mp4" if job.requested_format == "video" else "mp3"
        final_path = locate_final_media(job.workdir, ext)

        selected_video_fmt = find_format_by_id(info, job.format_id) if job.requested_format == "video" else None
        needs_trim = job.start_sec is not None or job.end_sec is not None
        needs_compat_transcode = job.requested_format == "video" and should_transcode_for_compatibility(selected_video_fmt)

        if needs_trim:
            set_job_state(job, status="processing", message="Обрезка по таймингу...")
            trimmed_path = job.workdir / f"clip.{ext}"
            trim_media(
                input_path=final_path,
                output_path=trimmed_path,
                media_kind=job.requested_format,
                start_sec=job.start_sec,
                end_sec=job.end_sec,
                mp3_bitrate=job.format_id if job.format_id.isdigit() else "192",
                should_cancel=lambda: should_cancel_job(job_id),
            )
            final_path = trimmed_path
        elif needs_compat_transcode:
            set_job_state(job, status="processing", message="Подготовка совместимого MP4...")
            compat_path = job.workdir / "compatible.mp4"
            transcode_video_to_h264(
                input_path=final_path,
                output_path=compat_path,
                should_cancel=lambda: should_cancel_job(job_id),
            )
            final_path = compat_path

        check_cancel()

        with JOB_LOCK:
            current = JOBS.get(job_id)
            if not current:
                cleanup_tree(job.workdir)
                return
            current.file_path = final_path
            current.download_name = f"{safe_title}.{ext}"
            current.status = "ready"
            current.message = "Файл готов к скачиванию"
            current.downloaded_bytes = None
            current.total_bytes = None
            touch_job(current)

    except JobCancelled:
        logger.info("Job %s cancelled", job_id)
        with JOB_LOCK:
            current = JOBS.get(job_id)
            if current:
                current.status = "cancelled"
                current.message = "Загрузка отменена"
                current.error = ""
                current.downloaded_bytes = None
                current.total_bytes = None
                touch_job(current)
        cleanup_tree(job.workdir)
    except BadRequest as exc:
        logger.warning("Prepare job validation error for %s: %s", job.url, exc)
        with JOB_LOCK:
            current = JOBS.get(job_id)
            if current:
                current.status = "error"
                current.message = "Ошибка подготовки"
                current.error = str(exc)
                touch_job(current)
        cleanup_tree(job.workdir)
    except DownloadError as exc:
        if should_cancel_job(job_id):
            with JOB_LOCK:
                current = JOBS.get(job_id)
                if current:
                    current.status = "cancelled"
                    current.message = "Загрузка отменена"
                    current.error = ""
                    touch_job(current)
            cleanup_tree(job.workdir)
            return
        logger.warning("yt-dlp prepare error for %s: %s", job.url, exc)
        with JOB_LOCK:
            current = JOBS.get(job_id)
            if current:
                current.status = "error"
                current.message = "Ошибка загрузки"
                current.error = str(exc)
                touch_job(current)
        cleanup_tree(job.workdir)
    except Exception as exc:
        logger.exception("Unexpected prepare error for %s", job.url)
        with JOB_LOCK:
            current = JOBS.get(job_id)
            if current:
                current.status = "error"
                current.message = "Внутренняя ошибка"
                current.error = str(exc)
                touch_job(current)
        cleanup_tree(job.workdir)


@app.get("/")
def index() -> str:
    return render_template("index.html")


@app.get("/healthz")
def healthz() -> Response:
    ffmpeg_available = shutil.which("ffmpeg") is not None
    return jsonify({"ok": True, "ffmpeg": ffmpeg_available, "jobs": len(JOBS)})


def _is_private_host(hostname: str) -> bool:
    try:
        for info in socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM):
            addr = info[4][0]
            ip = ipaddress.ip_address(addr)
            if ip.is_private or ip.is_loopback or ip.is_reserved or ip.is_link_local:
                return True
    except (socket.gaierror, ValueError):
        return True
    return False


@app.get("/api/thumb")
def api_thumb() -> Response:
    src = str(request.args.get("src") or "").strip()
    if not src:
        return Response("", status=400)

    parsed = urllib.parse.urlparse(src)
    if parsed.scheme not in {"http", "https"}:
        return Response("", status=400)

    hostname = parsed.hostname or ""
    if not hostname or _is_private_host(hostname):
        return Response("", status=403)

    req = urllib.request.Request(src, headers={
        "User-Agent": "Mozilla/5.0 ReClip/1.0",
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
    })
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            content = resp.read(MAX_THUMB_BYTES + 1)
            if len(content) > MAX_THUMB_BYTES:
                return Response("", status=413)
            content_type = resp.headers.get_content_type() or mimetypes.guess_type(src)[0] or "image/jpeg"
            response = Response(content, mimetype=content_type)
            response.headers["Cache-Control"] = "public, max-age=3600"
            return response
    except (urllib.error.URLError, TimeoutError, ValueError):
        return Response("", status=404)


@app.post("/api/info")
def api_info() -> Response:
    payload = request.get_json(silent=True) or {}
    url = str(payload.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL не указан"}), 400
    original_url = url
    url = normalize_source_url(url)

    try:
        info = extract_video_info(url)
        return jsonify(normalize_info_payload(info, original_url=original_url))
    except DownloadError as exc:
        logger.warning("yt-dlp info error for %s: %s", url, exc)
        return jsonify({"error": str(exc)}), 400
    except Exception:
        logger.exception("Unexpected /api/info error for %s", url)
        return jsonify({"error": "Внутренняя ошибка сервера"}), 500


@app.post("/api/prepare")
def api_prepare() -> Response:
    payload = request.get_json(silent=True) or {}
    url = str(payload.get("url") or "").strip()
    requested_format = str(payload.get("format") or "video").strip().lower()
    format_id = str(payload.get("format_id") or "").strip()
    requested_title = str(payload.get("title") or "").strip()

    if requested_format not in {"video", "mp3"}:
        raise BadRequest("Поддерживаются только video и mp3")
    if not url:
        raise BadRequest("URL не указан")
    url = normalize_source_url(url)

    start_sec = parse_optional_float(payload.get("start_sec"))
    end_sec = parse_optional_float(payload.get("end_sec"))
    if start_sec is not None and end_sec is not None and end_sec <= start_sec:
        raise BadRequest("Время окончания должно быть больше времени начала")

    job_id = uuid.uuid4().hex
    workdir = Path(tempfile.mkdtemp(prefix=f"job-{job_id}-", dir=JOBS_ROOT))
    job = JobState(
        job_id=job_id,
        url=url,
        requested_format=requested_format,
        format_id=format_id,
        requested_title=requested_title,
        start_sec=start_sec,
        end_sec=end_sec,
        workdir=workdir,
    )

    with JOB_LOCK:
        if len(JOBS) >= MAX_CONCURRENT_JOBS:
            cleanup_tree(workdir)
            return jsonify({"error": "Сервер перегружен, попробуйте позже"}), 429
        JOBS[job_id] = job

    thread = threading.Thread(target=run_prepare_job, args=(job_id,), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id, "status": job.status, "message": job.message})


@app.get("/api/job/<job_id>")
def api_job_status(job_id: str) -> Response:
    if not SAFE_JOB_ID_RE.fullmatch(job_id):
        return jsonify({"error": "Некорректный job_id"}), 400
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"error": "Задача не найдена"}), 404
        return jsonify(serialize_job(job))


@app.post("/api/cancel/<job_id>")
def api_cancel_job(job_id: str) -> Response:
    if not SAFE_JOB_ID_RE.fullmatch(job_id):
        return jsonify({"error": "Некорректный job_id"}), 400
    workdir_to_clean = None
    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return jsonify({"error": "Задача не найдена"}), 404
        job.cancel_requested = True
        if job.status == "ready":
            job.status = "cancelled"
            job.message = "Загрузка отменена"
            touch_job(job)
            workdir_to_clean = job.workdir
        else:
            touch_job(job)
    if workdir_to_clean:
        cleanup_tree(workdir_to_clean)
    return jsonify({"ok": True})


@app.get("/api/fetch/<job_id>")
def api_fetch_job(job_id: str) -> Response:
    if not SAFE_JOB_ID_RE.fullmatch(job_id):
        return Response("Некорректный job_id", status=400, mimetype="text/plain; charset=utf-8")

    with JOB_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return Response("Задача не найдена", status=404, mimetype="text/plain; charset=utf-8")
        file_path = job.file_path
        download_name = job.download_name
        if job.status != "ready" or not file_path or not file_path.exists():
            return Response("Файл ещё не готов", status=409, mimetype="text/plain; charset=utf-8")
        job.fetched = True
        touch_job(job)

    response = send_file(
        file_path,
        as_attachment=True,
        download_name=download_name,
        conditional=True,
        max_age=0,
    )
    response.call_on_close(lambda: remove_job(job_id))
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), debug=False)


def _purge_loop() -> None:
    while True:
        time.sleep(PURGE_INTERVAL_SECONDS)
        try:
            purge_expired_jobs()
        except Exception:
            logger.exception("Error in purge loop")


_purge_thread = threading.Thread(target=_purge_loop, daemon=True)
_purge_thread.start()


