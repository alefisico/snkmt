import time
from collections import deque
from typing import Dict, Any, Optional, List
from uuid import UUID
from pathlib import Path

from rich.markup import escape
from textual import work, on
from textual.app import ComposeResult
from textual.containers import VerticalScroll
from textual.css.query import NoMatches
from textual.reactive import reactive
from textual.widget import Widget
from textual.widgets import Label, Static

from snkmt.core.repository import WorkflowRepository
from snkmt.types.enums import Status
from snkmt.console.dask_monitor import (
    scan_log_files,
    resolve_dashboard_via_scheduler,
    query_metrics_remote,
    condor_counts_for_jobs,
)

BAR_WIDTH = 30
_HISTORY_MAXLEN = 60  # keep up to 60 samples (~60 s at 1 s poll rate)


def _rich_bar(counts: dict) -> str:
    """Return a Rich-markup coloured progress bar."""
    mem   = counts.get("memory",     0)
    proc  = counts.get("processing", 0)
    err   = counts.get("erred",      0)
    wait  = counts.get("waiting",    0)
    total = mem + proc + err + wait
    if total == 0:
        return f"[dim]{'·' * BAR_WIDTH}[/dim]  [dim]Mem:  -%  Mem+Run:  -%[/dim]"

    def cells(n: int) -> int:
        return max(0, int(round(n / total * BAR_WIDTH)))

    n_mem  = cells(mem)
    n_proc = cells(proc)
    n_err  = cells(err)
    n_wait = max(0, BAR_WIDTH - n_mem - n_proc - n_err)

    bar = (
        f"[green]{'=' * n_mem}[/green]"
        f"[cyan]{'-' * n_proc}[/cyan]"
        f"[red]{'x' * n_err}[/red]"
        f"[dim]{'.' * n_wait}[/dim]"
    )
    pct_mem = int(round(mem / total * 100))
    pct_run = int(round((mem + proc) / total * 100))
    workers = counts.get("workers", "?")
    return (
        f"|{bar}| "
        f"Mem:{pct_mem:3d}%  Mem+Run:{pct_run:3d}%  "
        f"[dim]workers={workers}  tasks={total}[/dim]"
    )


def _throughput_eta(history: deque, counts: dict):
    if len(history) < 2 or not counts:
        return None, None
    t_new, m_new = history[-1]
    t_base = m_base = None
    for t_old, m_old in history:
        if m_old < m_new:
            t_base, m_base = t_old, m_old
            break
    if t_base is None:
        return None, None
    dt = t_new - t_base
    if dt < 1:
        return None, None
    rate = (m_new - m_base) / dt
    mem  = counts.get("memory",     0)
    proc = counts.get("processing", 0)
    wait = counts.get("waiting",    0)
    remaining = proc + wait
    eta = remaining / rate if remaining > 0 else 0.0
    return rate, eta


def _fmt_eta(seconds: float) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60:02d}s"
    return f"{seconds // 3600}h {(seconds % 3600) // 60:02d}m"


class DaskJobPanel(VerticalScroll):
    """Live Dask task-progress for running jobs in the selected workflow."""

    workflow_id: reactive[Optional[UUID]] = reactive(None)

    def __init__(self, repo: WorkflowRepository, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.repo = repo
        self._metrics: dict = {}   # {job_name: {'counts': dict|None, 'status': str}}
        self._history: dict[str, deque] = {}   # {job_name: deque[(ts, memory)]}
        self._mounted = False

    def compose(self) -> ComposeResult:
        yield Static(
            "[dim]Waiting for Dask metrics...[/dim]",
            id="dask-content",
            markup=True,
        )

    def on_mount(self) -> None:
        self._mounted = True
        self.set_interval(1, self._poll)

    def watch_workflow_id(self) -> None:
        self._metrics = {}
        self._history = {}
        self._refresh_display()

    @work(exclusive=True)
    async def _poll(self) -> None:
        if self.workflow_id is None or not self._mounted:
            return

        try:
            # 1. Fetch running/active jobs from DB on main event loop
            jobs = await self.repo.list_jobs(self.workflow_id)
            if not jobs:
                self._metrics = {}
                self._refresh_display()
                return

            # Keep track of job statuses by log path
            job_statuses = {}
            log_paths = []
            for j in jobs:
                for lf in j.log_files:
                    job_statuses[lf.path] = j.status
                    log_paths.append(lf.path)

            if not log_paths:
                self._metrics = {}
                self._refresh_display()
                return

            # 2. Run blocking telemetry calls (log reading, HTTP, SSH, condor) in a thread worker
            worker = self.run_worker(
                self._fetch_telemetry_thread,
                log_paths,
                job_statuses,
                thread=True
            )
            telemetry_data = await worker.wait()

            if telemetry_data:
                self._process_telemetry_result(telemetry_data)

        except Exception as exc:
            import traceback
            self._show_error(repr(exc), traceback.format_exc())

    def _fetch_telemetry_thread(self, log_paths: List[str], job_statuses: Dict[str, Status]) -> Dict[str, Any]:
        """Runs in background thread: reads logs, queries prometheus/SSH, and queries condor."""
        scanned = scan_log_files(log_paths)
        new_metrics = {}

        for name, info in scanned.items():
            log_path = info.get("log_path", "")
            status = job_statuses.get(log_path)

            # Skip completed jobs or jobs no longer running
            if status != Status.RUNNING:
                continue
            if info.get("done"):
                continue

            dashboard_url = info.get("dashboard")
            if not dashboard_url and info.get("proxy_port") and info.get("scheduler"):
                host = info["scheduler"].split("://")[1].split(":")[0]
                dashboard_url = f"http://{host}:{info['proxy_port']}"

            # Query Dask metrics
            counts = query_metrics_remote(dashboard_url) if dashboard_url else None
            new_metrics[name] = {
                "counts": counts,
                "status": status,
                "has_dashboard": bool(dashboard_url)
            }

        # HTCondor worker counts
        condor = condor_counts_for_jobs(scanned)
        for name, cc in condor.items():
            if name in new_metrics:
                new_metrics[name]["condor"] = cc

        return new_metrics

    def _process_telemetry_result(self, new_metrics: Dict[str, Any]) -> None:
        """Processes metrics, updates memory/history, alerts errors, and refreshes UI."""
        now = time.monotonic()
        for name, info in new_metrics.items():
            mem = (info.get("counts") or {}).get("memory", 0)
            if name not in self._history:
                self._history[name] = deque(maxlen=_HISTORY_MAXLEN)
            self._history[name].append((now, mem))

        # Check for alerts
        for name, info in new_metrics.items():
            c = info.get("counts") or {}
            prev = (self._metrics.get(name) or {}).get("counts") or {}
            n_erred = c.get("erred", 0)
            if n_erred > prev.get("erred", 0):
                self.app.notify(
                    f"{name}: {n_erred} task{'s' if n_erred != 1 else ''} erred",
                    severity="error",
                    timeout=10,
                )
            n_paused = c.get("workers_paused", 0)
            if n_paused > 0 and prev.get("workers_paused", 0) == 0:
                self.app.notify(
                    f"{name}: {n_paused} worker{'s' if n_paused != 1 else ''} paused (memory pressure)",
                    severity="warning",
                    timeout=15,
                )

        self._metrics = new_metrics
        self._refresh_display()

    def _refresh_display(self) -> None:
        if not self._mounted:
            return
        try:
            content = self.query_one("#dask-content", Static)
        except NoMatches:
            return

        if self.workflow_id is None:
            content.update("[dim]Select a workflow to view running jobs.[/dim]")
            return

        if not self._metrics:
            content.update("[dim]No running Dask jobs detected.[/dim]")
            return

        lines = []
        for name, info in sorted(self._metrics.items()):
            counts = info.get("counts")
            n_erred = counts.get("erred", 0) if counts else 0

            if counts is None:
                if info.get("has_dashboard"):
                    reason = "[yellow]unreachable (SSH/network)[/yellow]"
                else:
                    reason = "[dim]waiting for dashboard...[/dim]"
                bar = f"[dim]{'·' * BAR_WIDTH}[/dim]  {reason}"
            else:
                bar = _rich_bar(counts)

            n_paused = counts.get("workers_paused", 0) if counts else 0
            name_str = (
                f"[bold red]⚠ {escape(name)}[/bold red]"
                if n_erred else
                f"[bold]{escape(name)}[/bold]"
            )
            erred_str  = f"\n  [bold red]⚠ {n_erred} task{'s' if n_erred != 1 else ''} erred[/bold red]" if n_erred else ""
            paused_str = f"\n  [bold yellow]⚠ {n_paused} worker{'s' if n_paused != 1 else ''} paused (memory pressure)[/bold yellow]" if n_paused else ""

            rate, eta = _throughput_eta(self._history.get(name, deque()), counts or {})
            if rate is not None:
                eta_str = f"  [dim]~{_fmt_eta(eta)} remaining[/dim]" if eta else "  [dim]nearly done[/dim]"
                throughput_str = f"\n  [dim]{rate:.1f} tasks/s{eta_str}[/dim]"
            elif counts is not None:
                throughput_str = "\n  [dim]measuring...[/dim]"
            else:
                throughput_str = ""

            condor = info.get("condor")
            if condor:
                idle, running, held = condor['idle'], condor['running'], condor['held']
                held_str = f"[bold red]{held}H[/bold red]" if held else f"[dim]{held}H[/dim]"
                idle_str = f"[yellow]{idle}I[/yellow]" if idle else f"[dim]{idle}I[/dim]"
                condor_str = f"\n  [dim]Condor:[/dim] {idle_str} [green]{running}R[/green] {held_str}"
            else:
                condor_str = ""

            lines.append(f"{name_str}\n  {bar}{erred_str}{paused_str}{throughput_str}{condor_str}")

        content.update("\n\n".join(lines))

    def _show_error(self, short: str, detail: str) -> None:
        try:
            content = self.query_one("#dask-content", Static)
            content.update(f"[red]Poll error:[/red] {escape(short)}\n\n[dim]{escape(detail)}[/dim]")
        except NoMatches:
            pass
