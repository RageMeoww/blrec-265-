from __future__ import annotations

import json
from loguru import logger
from subprocess import PIPE, Popen
from typing import Any, Dict, List, Literal, Optional, TypedDict, Union

from reactivex import Observable, abc
from reactivex.scheduler import CurrentThreadScheduler

__all__ = (
    'ffprobe',
    'ffprobe_on',
    'StreamProfile',
    'FormatProfile',
    'VideoProfile',
    'AudioProfile',
)


class VideoProfile(TypedDict, total=False):
    index: int
    codec_name: str
    codec_long_name: str
    codec_type: Literal['video']
    codec_tag_string: str
    codec_tag: str
    width: int
    height: int
    coded_width: int
    coded_height: int
    closed_captions: int
    film_grain: int
    has_b_frames: int
    level: int
    refs: int
    is_avc: str
    nal_length_size: str
    id: str
    r_frame_rate: str
    avg_frame_rate: str
    time_base: str
    duration_ts: int
    duration: str
    extradata_size: int
    disposition: Dict[str, int]
    tags: Dict[str, Any]


class AudioProfile(TypedDict, total=False):
    index: int
    codec_name: str
    codec_long_name: str
    codec_type: Literal['audio']
    codec_tag_string: str
    codec_tag: str
    sample_fmt: str
    sample_rate: str
    channels: int
    channel_layout: str
    bits_per_sample: int
    id: str
    r_frame_rate: str
    avg_frame_rate: str
    time_base: str
    duration_ts: int
    duration: str
    bit_rate: str
    extradata_size: int
    disposition: Dict[str, int]
    tags: Dict[str, Any]


class FormatProfile(TypedDict, total=False):
    filename: str
    nb_streams: int
    nb_programs: int
    format_name: str
    format_long_name: str
    size: str
    probe_score: int
    tags: Dict[str, Any]


class StreamProfile(TypedDict, total=False):
    streams: List[Union[VideoProfile, AudioProfile]]
    format: FormatProfile


def ffprobe(data: bytes) -> StreamProfile:
    args = [
        'ffprobe',
        '-show_streams',
        '-show_format',
        '-print_format',
        'json',
        'pipe:0',
    ]

    with Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE) as process:
        try:
            stdout, _stderr = process.communicate(data, timeout=10)
            profile = json.loads(stdout)
        except BaseException as e:
            logger.warning(f'[调试] ffprobe 报错: {type(e)} -> {e}')
            process.kill()
            process.wait()
            raise
        else:
            profile = normalize_streams(profile)
            return profile


def ffprobe_on(data: bytes) -> Observable[StreamProfile]:
    def subscribe(
        observer: abc.ObserverBase[StreamProfile],
        scheduler: Optional[abc.SchedulerBase] = None,
    ) -> abc.DisposableBase:
        _scheduler = scheduler or CurrentThreadScheduler()

        def action(scheduler: abc.SchedulerBase, state: Optional[Any] = None) -> None:
            try:
                profile = ffprobe(data)
            except Exception as e:
                logger.debug(f"[调试] ffprobe异常: {e}")
                observer.on_error(e)
            else:
                observer.on_next(profile)
                observer.on_completed()

        return _scheduler.schedule(action)

    return Observable(subscribe)


def normalize_streams(profile: dict) -> dict:
    streams = profile.get('streams', [])
    video_streams = [s for s in streams if s.get('codec_type') == 'video']
    audio_streams = [s for s in streams if s.get('codec_type') == 'audio']

    # ✨ 若 format 中有 encoder，则补到第一个视频流的 tags 中
    encoder = profile.get('format', {}).get('tags', {}).get('encoder')
    if video_streams and encoder:
        video_stream = video_streams[0]
        if 'tags' not in video_stream:
            video_stream['tags'] = {}
        if 'encoder' not in video_stream['tags']:
            video_stream['tags']['encoder'] = encoder

    if not video_streams:
        video_streams.append({
            'codec_type': 'video',
            'codec_name': 'none',
            'width': 0,
            'height': 0,
            'index': -1,
            'codec_long_name': '',
            'codec_tag_string': '',
            'codec_tag': '',
            'coded_width': 0,
            'coded_height': 0,
            'closed_captions': 0,
            'film_grain': 0,
            'has_b_frames': 0,
            'level': 0,
            'refs': 0,
            'is_avc': '',
            'nal_length_size': '',
            'id': '',
            'r_frame_rate': '',
            'avg_frame_rate': '',
            'time_base': '',
            'duration_ts': 0,
            'duration': '',
            'extradata_size': 0,
            'disposition': {},
            'tags': {},
        })
    if not audio_streams:
        audio_streams.append({
            'codec_type': 'audio',
            'codec_name': 'none',
            'sample_fmt': '',
            'sample_rate': '0',
            'channels': 0,
            'channel_layout': '',
            'bits_per_sample': 0,
            'index': -1,
            'codec_long_name': '',
            'codec_tag_string': '',
            'codec_tag': '',
            'id': '',
            'r_frame_rate': '',
            'avg_frame_rate': '',
            'time_base': '',
            'duration_ts': 0,
            'duration': '',
            'bit_rate': '',
            'extradata_size': 0,
            'disposition': {},
            'tags': {},
        })

    # 交替合并
    merged = []
    max_len = max(len(video_streams), len(audio_streams))
    for i in range(max_len):
        if i < len(video_streams):
            merged.append(video_streams[i])
        if i < len(audio_streams):
            merged.append(audio_streams[i])

    profile['streams'] = merged
    return profile
