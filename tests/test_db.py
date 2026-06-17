import pytest

from pathlib import Path

from snkmt.core.db.session import Database, AsyncDatabase
from snkmt.core.db.version import (
    get_database_revision,
    get_latest_revision,
    get_legacy_database_revision,
    is_legacy_database,
    stamp_legacy_database,
)
import tempfile
import sys

from pytest_loguru.plugin import caplog


@pytest.fixture
def temp_db_path():
    """Create a temporary database path."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir) / "test.db"


@pytest.fixture
async def async_db(temp_db_path):
    """Create an async database."""
    db = AsyncDatabase(db_path=str(temp_db_path), create_db=True)
    yield db
    await db.close()


def test_new_database_sets_latest_revision(temp_db_path):
    """Test that a new database is set to the latest revision."""

    db = Database(db_path=str(temp_db_path), create_db=True)

    actual_revision = db.get_revision()
    expected_revision = get_latest_revision()

    assert actual_revision == expected_revision


def test_new_database_creates_expected_tables(temp_db_path):
    """Test that a new database creates all expected tables."""
    db = Database(db_path=str(temp_db_path), create_db=True)
    
    # Get table info from the database
    db_info = db.get_db_info()
    tables = db_info["tables"]
    
    # Expected tables based on our models + alembic version table
    expected_tables = {
        "workflows",
        "rules", 
        "jobs",
        "files",
        "errors",
        "alembic_version"  # Alembic creates this table for version tracking
    }
    
    # Convert to set for easier comparison
    actual_tables = set(tables)
    
    # Check that all expected tables exist
    assert expected_tables.issubset(actual_tables), f"Missing tables: {expected_tables - actual_tables}"
    
    # Check that we don't have unexpected tables (allow extra tables but log them)
    extra_tables = actual_tables - expected_tables
    if extra_tables:
        print(f"Found extra tables (this might be okay): {extra_tables}")
    
    db.close()


@pytest.mark.asyncio
async def test_async_new_database_sets_latest_revision(temp_db_path):
    """Test that a new async database is set to the latest revision."""
    db = AsyncDatabase(db_path=str(temp_db_path), create_db=True)

    actual_revision = db.get_revision()  # Now sync method
    expected_revision = get_latest_revision()

    assert actual_revision == expected_revision
    await db.close()


@pytest.mark.asyncio
async def test_async_database_creates_file(temp_db_path):
    """Test that AsyncDatabase creates a database file."""
    assert not temp_db_path.exists()

    db = AsyncDatabase(db_path=str(temp_db_path), create_db=True)

    assert temp_db_path.exists()
    await db.close()


@pytest.mark.asyncio
async def test_async_database_raises_when_not_found():
    """Test that AsyncDatabase raises error when db doesn't exist and create_db=False."""
    from snkmt.core.db.session import DatabaseNotFoundError

    with tempfile.TemporaryDirectory() as temp_dir:
        fake_path = Path(temp_dir) / "nonexistent.db"

        with pytest.raises(DatabaseNotFoundError):
            _db = AsyncDatabase(db_path=str(fake_path), create_db=False)


@pytest.mark.asyncio
async def test_async_database_auto_migrates_outdated_revision(temp_db_path):
    """Test that AsyncDatabase auto-migrates when database revision is outdated."""
    from alembic.command import upgrade, downgrade
    from alembic.config import Config as AlembicConfig
    from pathlib import Path

    # First, create a database at latest revision
    db_sync = Database(
        db_path=str(temp_db_path),
        create_db=True,
        auto_migrate=True,
    )
    db_sync.close()

    # Set up Alembic and downgrade to old revision
    db_dir = Path(__file__).parent.parent / "src" / "snkmt" / "core" / "db"
    config = AlembicConfig(db_dir / "alembic.ini")
    config.set_main_option("script_location", str(db_dir / "alembic"))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{temp_db_path}")

    # Downgrade to old revision
    old_revision = "a088a7b93fe5"  # First revision from our migration history
    downgrade(config, old_revision)

    # Now create AsyncDatabase - it should auto-migrate via sync database
    db_async = AsyncDatabase(db_path=str(temp_db_path), create_db=False)

    # Should be at latest revision after auto-migration
    latest_revision = get_latest_revision()
    assert db_async.get_revision() == latest_revision

    await db_async.close()


def test_database_revision_functions():
    """Test the new revision-based functions."""
    from snkmt.core.db.version import get_latest_revision

    # Test that we can get the latest revision
    latest = get_latest_revision()
    assert latest is not None
    assert isinstance(latest, str)
    assert len(latest) == 12  # Alembic revision IDs are 12 characters


def test_legacy_database(temp_db_path, caplog):
    """
    this was the only database revision that made it to releases 0.1.0 to 0.1.2
    """

    import logging
    import shutil

    legacy_db = Path("tests/fixtures/legacy.db")
    test_db = Path(temp_db_path)
    shutil.copy(legacy_db, test_db)

    with caplog.at_level(logging.DEBUG):
        db = Database(
            db_path=str(temp_db_path),
            create_db=False,
            auto_migrate=True,
            ignore_version=False,
        )

        desired_rev = "a088a7b93fe5"

        assert (
            "Legacy database detected - auto-stamping with appropriate revision"
            in caplog.text
        )

        assert f"Legacy database stamped with revision: {desired_rev}" in caplog.text


@pytest.mark.asyncio
async def test_workflow_cascade_delete(temp_db_path):
    from uuid import uuid4
    from datetime import datetime, timezone
    from snkmt.types.dto import WorkflowDTO, CreateRuleDTO, CreateJobDTO
    from snkmt.types.enums import Status

    async_db = AsyncDatabase(db_path=str(temp_db_path), create_db=True)
    repo = async_db.get_workflow_repository()

    wf_id = uuid4()
    now = datetime.now(timezone.utc)
    wf_dto = WorkflowDTO(
        id=wf_id,
        status=Status.RUNNING,
        name="test_workflow",
        total_job_count=10,
        jobs_finished=0,
        started_at=now,
        updated_at=now,
        dryrun=False,
    )

    # Create workflow
    created_wf_id = await repo.create(wf_dto)
    assert created_wf_id == wf_id

    # Create rule
    rule_id = await repo.create_rule(wf_id, CreateRuleDTO(name="test_rule", total_job_count=5))
    assert rule_id is not None

    # Create job
    job = await repo.create_job(
        wf_id,
        rule_id,
        CreateJobDTO(
            snakemake_id=1,
            status=Status.RUNNING,
            threads=1,
            started_at=now,
        )
    )
    assert job is not None

    # Verify they exist
    wf = await repo.get(wf_id)
    assert wf is not None

    rules = await repo.list_rules(wf_id, status=None)
    assert len(rules) == 1

    jobs = await repo.list_rule_jobs(wf_id, rule_id)
    assert len(jobs) == 1

    # Now delete workflow
    success = await repo.delete(wf_id)
    assert success is True

    # Verify it is deleted
    assert await repo.get(wf_id) is None

    # Let's check rules and jobs are also gone in the DB
    async with async_db.get_session()() as session:
        from sqlalchemy import select
        from snkmt.core.models import Rule, Job

        rules_in_db = (await session.execute(select(Rule).where(Rule.workflow_id == wf_id))).scalars().all()
        assert len(rules_in_db) == 0

        jobs_in_db = (await session.execute(select(Job).where(Job.workflow_id == wf_id))).scalars().all()
        assert len(jobs_in_db) == 0

    await async_db.close()


@pytest.mark.asyncio
async def test_workflow_prune(temp_db_path):
    from uuid import uuid4
    from datetime import datetime, timedelta, timezone
    from snkmt.types.dto import WorkflowDTO
    from snkmt.types.enums import Status

    async_db = AsyncDatabase(db_path=str(temp_db_path), create_db=True)
    repo = async_db.get_workflow_repository()

    # Create one old workflow and one new workflow
    wf1_id = uuid4()
    now = datetime.now(timezone.utc)
    wf1 = WorkflowDTO(
        id=wf1_id,
        status=Status.SUCCESS,
        name="old_wf",
        total_job_count=1,
        jobs_finished=1,
        started_at=now - timedelta(days=10),
        updated_at=now - timedelta(days=10),
        dryrun=False,
    )
    await repo.create(wf1)

    wf2_id = uuid4()
    wf2 = WorkflowDTO(
        id=wf2_id,
        status=Status.RUNNING,
        name="new_wf",
        total_job_count=1,
        jobs_finished=0,
        started_at=now,
        updated_at=now,
        dryrun=False,
    )
    await repo.create(wf2)

    # Prune workflows older than 5 days
    cutoff = datetime.now(timezone.utc) - timedelta(days=5)
    deleted = await repo.prune(before_date=cutoff)
    assert deleted == 1

    # Check wf1 is deleted, wf2 is not
    assert await repo.get(wf1_id) is None
    assert await repo.get(wf2_id) is not None

    await async_db.close()


@pytest.mark.asyncio
async def test_resolve_real_snakefile(temp_db_path):
    import os
    from uuid import uuid4
    from datetime import datetime, timezone
    from snkmt.types.dto import WorkflowDTO, CreateRuleDTO
    from snkmt.types.enums import Status
    from snkmt.core.repository.sql import SQLAlchemyWorkflowRepository

    # Reset rule map cache first to ensure it scans
    SQLAlchemyWorkflowRepository._rule_map_cache = None
    SQLAlchemyWorkflowRepository._snakefile_cache = {}

    # Create a dummy smk file in current dir
    dummy_smk = "temp_resolve_test_workflow.smk"
    with open(dummy_smk, "w") as f:
        f.write("rule resolve_test_rule_1:\n    input: 'a'\n")

    try:
        async_db = AsyncDatabase(db_path=str(temp_db_path), create_db=True)
        repo = async_db.get_workflow_repository()

        wf_id = uuid4()
        now = datetime.now(timezone.utc)
        wf_dto = WorkflowDTO(
            id=wf_id,
            status=Status.RUNNING,
            name="test_workflow",
            snakefile="path/to/snakemake/workflow.py",
            total_job_count=10,
            jobs_finished=0,
            started_at=now,
            updated_at=now,
            dryrun=False,
        )

        # Create workflow and rule
        await repo.create(wf_dto)
        await repo.create_rule(wf_id, CreateRuleDTO(name="resolve_test_rule_1", total_job_count=5))

        # Retrieve workflow - should resolve real snakefile
        wf = await repo.get(wf_id)
        assert wf is not None
        assert wf.snakefile is not None
        assert wf.snakefile.endswith(dummy_smk)

        # Verify caching works - modify rule_map_cache and check it is used
        assert SQLAlchemyWorkflowRepository._rule_map_cache is not None
        SQLAlchemyWorkflowRepository._rule_map_cache["resolve_test_rule_1"] = ["/fake/path/fake.smk"]
        # Clear snakefile cache to force resolution again
        SQLAlchemyWorkflowRepository._snakefile_cache = {}
        wf = await repo.get(wf_id)
        assert wf is not None
        assert wf.snakefile == "/fake/path/fake.smk"

        await async_db.close()
    finally:
        if os.path.exists(dummy_smk):
            os.remove(dummy_smk)


