import os
from pathlib import Path
from dataclasses import dataclass
from typing import List
from snkmt.types.enums import Status, FileType
from snkmt.types.dto import FileDTO
from snkmt.console.dask_monitor import (
    parse_condor_event_status,
    _is_matching_condor_file,
    find_condor_logs,
)


@dataclass
class MockFileDTO:
    path: str
    file_type: FileType


@dataclass
class MockJobDTO:
    log_files: List[MockFileDTO]


def test_parse_condor_event_status():
    # Test SUCCESS event
    success_content = """000 (123.000.000) 06/11 12:00:00 Job submitted
001 (123.000.000) 06/11 12:01:00 Job executing
005 (123.000.000) 06/11 12:10:00 Job terminated.
    (1) Normal termination (return value 0)
"""
    assert parse_condor_event_status(success_content) == "SUCCESS"

    # Test ERROR event
    failed_content = """000 (123.000.000) 06/11 12:00:00 Job submitted
001 (123.000.000) 06/11 12:01:00 Job executing
005 (123.000.000) 06/11 12:10:00 Job terminated.
    (1) Normal termination (return value 1)
"""
    assert parse_condor_event_status(failed_content) == "ERROR"

    # Test HELD event
    held_content = """000 (123.000.000) 06/11 12:00:00 Job submitted
012 (123.000.000) 06/11 12:01:00 Job held.
"""
    assert parse_condor_event_status(held_content) == "ERROR"

    # Test RUNNING event
    running_content = """000 (123.000.000) 06/11 12:00:00 Job submitted
001 (123.000.000) 06/11 12:01:00 Job executing
"""
    assert parse_condor_event_status(running_content) == "RUNNING"


def test_is_matching_condor_file():
    assert _is_matching_condor_file("2.out", "2", "2.txt") is True
    assert _is_matching_condor_file("2.err", "2", "2.txt") is True
    assert _is_matching_condor_file("2.txt.condor.log", "2", "2.txt") is True
    assert _is_matching_condor_file("20.txt", "2", "2.txt") is False
    assert _is_matching_condor_file("2.txt", "2", "2.txt") is False  # Ignore self


def test_find_condor_logs_manual(tmp_path):
    # Setup files in tmp_path
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    
    main_log = log_dir / "analysis_lowpt.log"
    main_log.write_text("Some snakemake logs")
    
    condor_log = log_dir / "analysis_lowpt.condor.log"
    condor_log.write_text("""000 (100.000.000) 06/11 12:00:00 Job submitted
001 (100.000.000) 06/11 12:01:00 Job executing
005 (100.000.000) 06/11 12:10:00 Job terminated.
    (1) Normal termination (return value 0)
""")
    
    condor_out = log_dir / "analysis_lowpt.out"
    condor_out.write_text("stdout content")
    condor_err = log_dir / "analysis_lowpt.err"
    condor_err.write_text("stderr content")
    
    # Unrelated files
    unrelated_out = log_dir / "other.out"
    unrelated_out.write_text("unrelated")

    job = MockJobDTO(
        log_files=[MockFileDTO(path=str(main_log), file_type=FileType.LOG)]
    )
    
    results = find_condor_logs(job)
    
    # We expect condor_log, condor_out, condor_err to be found
    # We do NOT expect unrelated_out or main_log to be returned as condor logs
    names = [r["name"] for r in results]
    assert "analysis_lowpt.condor.log" in names
    assert "analysis_lowpt.out" in names
    assert "analysis_lowpt.err" in names
    assert "other.out" not in names
    assert "analysis_lowpt.log" not in names

    # Check status association
    for r in results:
        if r["name"] == "analysis_lowpt.condor.log":
            assert r["status"] == Status.SUCCESS
            assert r["type"] == "Event Log"
        elif r["name"] == "analysis_lowpt.out":
            assert r["status"] == Status.SUCCESS
            assert r["type"] == "Stdout"


def test_find_condor_logs_dask(tmp_path):
    # Setup files in tmp_path
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    
    worker_dir = tmp_path / "workers"
    worker_dir.mkdir()
    
    main_log = log_dir / "analysis_dask.log"
    main_log.write_text(f"Condor worker log directory: {worker_dir}")
    
    worker_log = worker_dir / "condor.100.0.log"
    worker_log.write_text("""000 (100.000.000) 06/11 12:00:00 Job submitted
001 (100.000.000) 06/11 12:01:00 Job executing
""")
    worker_out = worker_dir / "condor.100.0.out"
    worker_out.write_text("worker stdout")

    job = MockJobDTO(
        log_files=[MockFileDTO(path=str(main_log), file_type=FileType.LOG)]
    )

    results = find_condor_logs(job)
    
    names = [r["name"] for r in results]
    assert "condor.100.0.log" in names
    assert "condor.100.0.out" in names
    
    # Check status is running
    for r in results:
        assert r["status"] == Status.RUNNING
