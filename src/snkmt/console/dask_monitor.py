import os
import re
import glob
import subprocess
import urllib.request
import urllib.error
import json
from typing import Dict, List, Any, Optional

DASHBOARD_RE = re.compile(r"Dask dashboard:\s+(http://\S+)")
PROXY_RE = re.compile(r"Dask dashboard:\s+/proxy/(\d+)")  # /proxy/PORT/status
SCHEDULER_RE = re.compile(
    r"'tcp://([^']+)'"
)  # matches tcp://host:port inside Client repr
COMPLETE_RE = re.compile(
    r"JOB EXECUTION COMPLETED SUCCESSFULLY|Dask performance report saved"
)
WORKER_LOG_DIR_RE = re.compile(r"Condor worker log directory: (\S+)")

TASK_METRIC_RE = re.compile(r'dask_scheduler_tasks\{state="(\w+)"\}\s+([\d.]+)')
WORKER_METRIC_RE = re.compile(r'dask_scheduler_workers\{state="(\w+)"\}\s+([\d.]+)')

_dashboard_cache: Dict[str, Optional[str]] = {}
_SSH_CTL_FMT = "/tmp/barista_ssh_ctl_%h"


def scan_log_files(paths: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Parse a list of log file paths and extract dashboard URLs, scheduler TCP addresses,
    and completion status.

    Returns:
        {job_name: {'dashboard': str, 'scheduler': str, 'done': bool, 'log_path': str}}
    """
    jobs = {}
    for path in sorted(paths):
        if not os.path.exists(path):
            continue
        name = os.path.basename(path).replace(".log", "")
        info = {}
        try:
            with open(path, "r", errors="replace") as f:
                for line in f:
                    m = DASHBOARD_RE.search(line)
                    if m:
                        info["dashboard"] = m.group(1).rstrip("/status").rstrip("/")
                        info.pop("proxy_port", None)
                        info.pop(
                            "done", None
                        )  # new run started — clear stale completion
                    else:
                        m = PROXY_RE.search(line)
                        if m:
                            info["proxy_port"] = m.group(1)
                            info.pop(
                                "done", None
                            )  # new run started — clear stale completion
                    m = SCHEDULER_RE.search(line)
                    if m:
                        info["scheduler"] = f"tcp://{m.group(1)}"
                    m = WORKER_LOG_DIR_RE.search(line)
                    if m:
                        info["worker_log_dir"] = m.group(1)
                    if COMPLETE_RE.search(line):
                        info["done"] = True
        except OSError:
            pass
        if info:
            info["log_path"] = os.path.abspath(path)
            jobs[name] = info
    return jobs


def resolve_dashboard_via_scheduler(
    scheduler_addr: str, timeout: int = 3
) -> Optional[str]:
    """Connect to a running Dask scheduler and retrieve its dashboard URL."""
    if scheduler_addr in _dashboard_cache:
        return _dashboard_cache[scheduler_addr]
    try:
        from distributed import Client

        c = Client(scheduler_addr, timeout=timeout, set_as_default=False)
        url = c.dashboard_link.rstrip("/status").rstrip("/")
        c.close()
        _dashboard_cache[scheduler_addr] = url
        return url
    except Exception:
        return None


def query_metrics(base_url: str, timeout: int = 2) -> Optional[Dict[str, Any]]:
    """
    Fetch /metrics from the Dask dashboard and return task/worker counts.
    """
    url = f"{base_url}/metrics"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, OSError):
        return None

    counts = {}
    for m in TASK_METRIC_RE.finditer(text):
        counts[m.group(1)] = int(float(m.group(2)))

    worker_states = {}
    for m in WORKER_METRIC_RE.finditer(text):
        worker_states[m.group(1)] = int(float(m.group(2)))
    if worker_states:
        counts["workers"] = sum(worker_states.values())
        counts["workers_busy"] = worker_states.get(
            "partially_saturated", 0
        ) + worker_states.get("saturated", 0)
        counts["workers_paused"] = worker_states.get("paused", 0)

    return counts if counts else None


def query_metrics_remote(base_url: str, timeout: int = 5) -> Optional[Dict[str, Any]]:
    """Like query_metrics but falls back to SSH when direct HTTP is unreachable."""
    counts = query_metrics(base_url, timeout=2)
    if counts is not None:
        return counts

    host_match = re.match(r"https?://([^/:]+):(\d+)", base_url)
    if not host_match:
        return None
    host, port = host_match.group(1), host_match.group(2)

    try:
        result = subprocess.run(
            [
                "ssh",
                "-o",
                "ControlMaster=auto",
                "-o",
                f"ControlPath={_SSH_CTL_FMT}",
                "-o",
                "ControlPersist=120",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "BatchMode=yes",
                "-o",
                f"ConnectTimeout={timeout}",
                host,
                f"curl -sf --max-time {timeout} http://localhost:{port}/metrics",
            ],
            capture_output=True,
            text=True,
            timeout=timeout + 3,
        )
        if result.returncode != 0 or not result.stdout:
            return None
        text = result.stdout
    except (subprocess.TimeoutExpired, OSError):
        return None

    counts = {}
    for m in TASK_METRIC_RE.finditer(text):
        counts[m.group(1)] = int(float(m.group(2)))
    worker_states = {}
    for m in WORKER_METRIC_RE.finditer(text):
        worker_states[m.group(1)] = int(float(m.group(2)))
    if worker_states:
        counts["workers"] = sum(worker_states.values())
        counts["workers_busy"] = worker_states.get(
            "partially_saturated", 0
        ) + worker_states.get("saturated", 0)
        counts["workers_paused"] = worker_states.get("paused", 0)
    return counts if counts else None


_CONDOR_STATUS = {1: "idle", 2: "running", 5: "held"}


def condor_counts_for_jobs(
    scanned: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, int]]:
    """Return HTCondor worker counts per job based on worker log directories."""
    dir_to_job = {}
    for name, info in scanned.items():
        wld = info.get("worker_log_dir")
        if wld:
            dir_to_job[wld.rstrip("/")] = name

    if not dir_to_job:
        return {}

    try:
        out = subprocess.check_output(
            "condor_q -json",
            shell=True,
            stderr=subprocess.DEVNULL,
            universal_newlines=True,
            timeout=5,
        )
        all_jobs = json.loads(out) if out.strip() else []
    except Exception:
        return {}

    counts = {}  # {job_name: {'idle': 0, 'running': 0, 'held': 0}}

    for j in all_jobs:
        status = j.get("JobStatus", 0)
        key = _CONDOR_STATUS.get(status)
        if key is None:
            continue
        out_path = j.get("Out", "") or j.get("Err", "")
        for wld, job_name in dir_to_job.items():
            if out_path.startswith(wld):
                entry = counts.setdefault(
                    job_name, {"idle": 0, "running": 0, "held": 0}
                )
                entry[key] += 1
                break

    return counts
