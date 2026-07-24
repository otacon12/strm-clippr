#!/usr/bin/env python3
"""OBS busy-state guard for compute-heavy workers.

D-009: refuse heavy local jobs while OBS is actively streaming/recording,
unless explicitly overridden by CLPR_ALLOW_DURING_STREAM=1.
"""

from __future__ import annotations

import json
import os
import socket
from pathlib import Path

OBS_HOST = '127.0.0.1'
OBS_PORT = 4455
OBS_CONNECT_TIMEOUT_S = 1.5
OBS_CONFIG_PATH = Path.home() / 'Library/Application Support/obs-studio/plugin_config/obs-websocket/config.json'


def _read_obs_websocket_password() -> str:
    if not OBS_CONFIG_PATH.exists():
        raise RuntimeError(
            f'OBS websocket config not found at {OBS_CONFIG_PATH}. '
            'If OBS is running, restore obs-websocket config. '
            'If you must proceed intentionally, set CLPR_ALLOW_DURING_STREAM=1.'
        )

    payload = json.loads(OBS_CONFIG_PATH.read_text(encoding='utf-8'))
    password = (payload.get('server_password') or '').strip()
    if password == '':
        raise RuntimeError(
            f'OBS websocket config missing non-empty server_password at {OBS_CONFIG_PATH}. '
            'Set the OBS websocket password in OBS settings. '
            'If you must proceed intentionally, set CLPR_ALLOW_DURING_STREAM=1.'
        )
    return password


def _port_open(host: str, port: int, timeout_s: float) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


def _is_active(status: object, *names: str) -> bool:
    for n in names:
        v = getattr(status, n, None)
        if isinstance(v, bool):
            if v:
                return True
        elif isinstance(v, (int, float)):
            if int(v) != 0:
                return True
    return False


def require_obs_idle_or_raise(worker_name: str) -> None:
    if os.environ.get('CLPR_ALLOW_DURING_STREAM') == '1':
        print(
            f'WARNING OBS_GUARD_OVERRIDE worker={worker_name} '
            'D-009 protection disabled by CLPR_ALLOW_DURING_STREAM=1'
        )
        return

    try:
        import obsws_python as obsws
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            'obs_guard cannot determine live OBS state because obsws-python is not installed. '
            'Install it with: python3 -m pip install obsws-python. '
            'If this run is intentional, set CLPR_ALLOW_DURING_STREAM=1 to override D-009.'
        ) from exc

    # OBS closed/unreachable is explicitly the safe path.
    if not _port_open(OBS_HOST, OBS_PORT, OBS_CONNECT_TIMEOUT_S):
        print(f'OBS_GUARD_OK worker={worker_name} OBS not reachable at {OBS_HOST}:{OBS_PORT}; safe to proceed')
        return

    password = _read_obs_websocket_password()

    try:
        client = obsws.ReqClient(
            host=OBS_HOST,
            port=OBS_PORT,
            password=password,
            timeout=OBS_CONNECT_TIMEOUT_S,
        )
        stream_status = client.get_stream_status()
        record_status = client.get_record_status()
    except Exception as exc:
        raise RuntimeError(
            'obs_guard failed to query live OBS stream/record state. '
            'Resolve OBS websocket connectivity/auth, or set CLPR_ALLOW_DURING_STREAM=1 for an explicit override. '
            f'Underlying error: {exc}'
        ) from exc

    stream_active = _is_active(stream_status, 'output_active', 'stream_active')
    record_active = _is_active(record_status, 'output_active', 'record_active')

    if stream_active or record_active:
        states = []
        if stream_active:
            states.append('streaming')
        if record_active:
            states.append('recording')
        state_text = ' and '.join(states)
        raise RuntimeError(
            f'OBS is actively {state_text}. Refusing {worker_name}. '
            'Finish the stream/record first, or set CLPR_ALLOW_DURING_STREAM=1 for an explicit override.'
        )

    print(f'OBS_GUARD_OK worker={worker_name} OBS reachable and idle; safe to proceed')
