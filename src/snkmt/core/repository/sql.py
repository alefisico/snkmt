from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy import select, and_, func, case
from sqlalchemy.orm import selectinload
from typing import Optional, List, Union
from datetime import datetime, timezone, timedelta
from uuid import UUID

from snkmt.core.models import Workflow, Rule, Job, File
from snkmt.types.enums import Status, DateFilter
from snkmt.core.repository import WorkflowRepository
from snkmt.types.dto import (
    JobCounts,
    WorkflowDTO,
    RuleDTO,
    JobDTO,
    UpdateWorkflowDTO,
    CreateFileDTO,
    CreateJobDTO,
    CreateRuleDTO,
    UpdateJobDTO,
    UpdateRuleDTO,
    FileDTO,
)


class SQLAlchemyWorkflowRepository(WorkflowRepository):
    _snakefile_cache = {}
    _rule_map_cache = None

    def __init__(self, session_factory: async_sessionmaker):
        self.async_session = session_factory

    async def _resolve_real_snakefile(
        self, session, workflow_id: UUID
    ) -> Optional[str]:
        if workflow_id in SQLAlchemyWorkflowRepository._snakefile_cache:
            return SQLAlchemyWorkflowRepository._snakefile_cache[workflow_id]

        import re
        import os
        from pathlib import Path
        from snkmt.core.models import Rule

        # 1. Fetch rule names for this workflow
        rules_result = await session.execute(
            select(Rule.name).where(Rule.workflow_id == workflow_id)
        )
        rule_names = [r[0] for r in rules_result.fetchall() if r[0]]
        if not rule_names:
            return None

        # 2. Build the rule-to-file map of the workspace if not cached
        if SQLAlchemyWorkflowRepository._rule_map_cache is None:
            rule_map = {}
            rule_re = re.compile(r"^\s*rule\s+(\w+)\s*:")

            exclude_dirs = {
                ".git",
                ".pixi",
                ".venv",
                ".snakemake",
                "node_modules",
                "__pycache__",
                ".mypy_cache",
                ".pytest_cache",
                ".ruff_cache",
                "build",
                "dist",
                "results",
                "data",
                "logs",
                "output",
                "outputs",
                "resources",
                "inputs",
                "test_temp",
            }

            for root, dirs, files in os.walk("."):
                # Prune excluded directories and hidden directories starting with "." in-place
                dirs[:] = [
                    d for d in dirs
                    if d.lower() not in exclude_dirs and not d.startswith(".")
                ]

                for f in files:
                    if f.endswith(".smk") or f.startswith("Snakefile"):
                        abs_path = os.path.abspath(os.path.join(root, f))
                        try:
                            with open(abs_path, "r", errors="ignore") as file_obj:
                                for line in file_obj:
                                    m = rule_re.match(line)
                                    if m:
                                        rule_name = m.group(1)
                                        rule_map.setdefault(rule_name, []).append(abs_path)
                        except OSError:
                            pass
            SQLAlchemyWorkflowRepository._rule_map_cache = rule_map

        # 3. Score each .smk file by how many rules it matches
        scores = {}
        for rule in rule_names:
            matching_files = SQLAlchemyWorkflowRepository._rule_map_cache.get(rule, [])
            for f in matching_files:
                scores[f] = scores.get(f, 0) + 1

        if not scores:
            return None

        # Find the file with the highest match score
        best_file = max(scores, key=scores.get)
        SQLAlchemyWorkflowRepository._snakefile_cache[workflow_id] = best_file
        return best_file

    async def get(self, workflow_id: UUID) -> Optional[WorkflowDTO]:
        async with self.async_session() as session:
            result = await session.execute(
                select(Workflow).where(Workflow.id == workflow_id)
            )
            workflow = result.scalar_one_or_none()
            if not workflow:
                return None
            dto = self._workflow_to_dto(workflow)
            if dto.snakefile and "snakemake/workflow.py" in dto.snakefile:
                real_snakefile = await self._resolve_real_snakefile(
                    session, workflow.id
                )
                if real_snakefile:
                    dto.snakefile = real_snakefile
            return dto

    async def delete(self, workflow_id: UUID) -> bool:
        async with self.async_session() as session:
            workflow = await session.get(Workflow, workflow_id)
            if workflow:
                await session.delete(workflow)
                await session.commit()
                return True
            return False

    async def create(self, workflow: WorkflowDTO) -> Optional[UUID]:
        async with self.async_session() as session:
            new_workflow = Workflow(
                id=workflow.id,
                snakefile=workflow.snakefile,
                status=workflow.status,
                total_job_count=workflow.total_job_count,
                jobs_finished=workflow.jobs_finished,
                started_at=workflow.started_at,
                updated_at=workflow.updated_at,
                dryrun=workflow.dryrun,
                command_line=workflow.command_line,
            )
            wf_id = new_workflow.id
            session.add(new_workflow)
            await session.commit()
            return wf_id

    async def update(self, update: UpdateWorkflowDTO) -> bool:
        async with self.async_session() as session:
            workflow = await session.get(Workflow, update.id)
            if not workflow:
                return False

            if update.status is not None:
                workflow.status = update.status
            if update.total_job_count is not None:
                workflow.total_job_count = update.total_job_count
            if update.jobs_finished is not None:
                workflow.jobs_finished = update.jobs_finished
            if update.end_time is not None:
                workflow.end_time = update.end_time

            await session.commit()
            return True

    async def list(
        self,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by: str = "started_at",
        descending: bool = True,
        since: Optional[datetime] = None,
        name: Optional[str] = None,
        status: Optional[Union[str, Status]] = None,
        started_at: Optional[DateFilter] = None,
    ) -> List[WorkflowDTO]:
        async with self.async_session() as session:
            stmt = select(Workflow)

            if since:
                stmt = stmt.where(Workflow.updated_at >= since)

            if name:
                stmt = stmt.where(Workflow.snakefile.ilike(f"%{name}%"))

            if status and status != "all":
                if isinstance(status, str):
                    status = Status(status.upper())
                stmt = stmt.where(Workflow.status == status)

            if started_at and started_at != DateFilter.ANY:
                date_condition = self._get_date_condition(started_at)
                if date_condition is not None:
                    stmt = stmt.where(date_condition)

            order_column = getattr(Workflow, order_by, Workflow.started_at)
            stmt = stmt.order_by(order_column.desc() if descending else order_column)

            if limit:
                stmt = stmt.limit(limit)
            if offset:
                stmt = stmt.offset(offset)

            result = await session.execute(stmt)
            workflows = result.scalars().all()
            dtos = []
            for w in workflows:
                dto = self._workflow_to_dto(w)
                if dto.snakefile and "snakemake/workflow.py" in dto.snakefile:
                    real_snakefile = await self._resolve_real_snakefile(session, w.id)
                    if real_snakefile:
                        dto.snakefile = real_snakefile
                dtos.append(dto)
            return dtos

    async def count(
        self,
        name: Optional[str] = None,
        status: Optional[Union[str, Status]] = None,
        started_at: Optional[DateFilter] = None,
    ) -> int:
        async with self.async_session() as session:
            stmt = select(func.count(Workflow.id))

            if name:
                stmt = stmt.where(Workflow.snakefile.ilike(f"%{name}%"))

            if status and status != "all":
                if isinstance(status, str):
                    status = Status(status.upper())
                stmt = stmt.where(Workflow.status == status)

            if started_at and started_at != DateFilter.ANY:
                date_condition = self._get_date_condition(started_at)
                if date_condition is not None:
                    stmt = stmt.where(date_condition)

            result = await session.execute(stmt)
            return result.scalar() or 0

    def _get_date_condition(self, date_filter: DateFilter):
        """Convert DateFilter to SQLAlchemy condition."""
        now = datetime.now(timezone.utc)
        now_date = now.date()

        match date_filter:
            case DateFilter.TODAY:
                start_of_day = datetime.combine(now_date, datetime.min.time()).replace(
                    tzinfo=timezone.utc
                )
                end_of_day = datetime.combine(now_date, datetime.max.time()).replace(
                    tzinfo=timezone.utc
                )
                return and_(
                    Workflow.started_at >= start_of_day,
                    Workflow.started_at <= end_of_day,
                )
            case DateFilter.YESTERDAY:
                yesterday = now_date - timedelta(days=1)
                start_of_day = datetime.combine(yesterday, datetime.min.time()).replace(
                    tzinfo=timezone.utc
                )
                end_of_day = datetime.combine(yesterday, datetime.max.time()).replace(
                    tzinfo=timezone.utc
                )
                return and_(
                    Workflow.started_at >= start_of_day,
                    Workflow.started_at <= end_of_day,
                )
            case DateFilter.LAST_7_DAYS:
                seven_days_ago = now - timedelta(days=7)
                return Workflow.started_at >= seven_days_ago
            case DateFilter.LAST_30_DAYS:
                thirty_days_ago = now - timedelta(days=30)
                return Workflow.started_at >= thirty_days_ago
            case DateFilter.LAST_90_DAYS:
                ninety_days_ago = now - timedelta(days=90)
                return Workflow.started_at >= ninety_days_ago
            case DateFilter.THIS_YEAR:
                start_of_year = datetime(now_date.year, 1, 1, tzinfo=timezone.utc)
                return Workflow.started_at >= start_of_year
            case _:
                return None

    async def list_rules(
        self,
        workflow_id: UUID,
        status: Optional[Status] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by: str = "updated_at",
        descending: bool = True,
        since: Optional[datetime] = None,
    ) -> List[RuleDTO]:
        async with self.async_session() as session:
            stmt = select(Rule).where(Rule.workflow_id == workflow_id)

            if since:
                stmt = stmt.where(Rule.updated_at >= since)

            # Filter by status requires joining with jobs
            if status:
                stmt = stmt.join(Job).where(Job.status == status).distinct()

            order_column = getattr(Rule, order_by, Rule.updated_at)
            stmt = stmt.order_by(order_column.desc() if descending else order_column)

            if limit:
                stmt = stmt.limit(limit)
            if offset:
                stmt = stmt.offset(offset)

            result = await session.execute(stmt)
            rules = result.scalars().all()

            if rules:
                rule_ids = [r.id for r in rules]
                counts_result = await session.execute(
                    select(
                        Job.rule_id,
                        func.sum(
                            case((Job.status == Status.RUNNING, 1), else_=0)
                        ).label("running"),
                        func.sum(case((Job.status == Status.ERROR, 1), else_=0)).label(
                            "failed"
                        ),
                        func.sum(
                            case((Job.status == Status.SUCCESS, 1), else_=0)
                        ).label("success"),
                    )
                    .where(Job.rule_id.in_(rule_ids))
                    .group_by(Job.rule_id)
                )

                counts_by_rule = {}
                for row in counts_result:
                    counts_by_rule[row.rule_id] = {
                        "running": row.running or 0,
                        "failed": row.failed or 0,
                        "success": row.success or 0,
                    }

                dtos = []
                for rule in rules:
                    job_counts_dict = counts_by_rule.get(
                        rule.id, {"running": 0, "failed": 0, "success": 0}
                    )
                    pending = rule.total_job_count - sum(job_counts_dict.values())
                    job_counts_dict["pending"] = pending
                    job_counts_dict["total"] = rule.total_job_count
                    job_counts = JobCounts(**job_counts_dict)
                    dtos.append(self._rule_to_dto(rule, job_counts))
                return dtos

            return []

    async def list_rule_jobs(self, workflow_id: UUID, rule_id: int) -> List[JobDTO]:
        async with self.async_session() as session:
            stmt = (
                select(Job)
                .options(selectinload(Job.files))
                .join(Rule)
                .where(and_(Rule.workflow_id == workflow_id, Job.rule_id == rule_id))
            )
            result = await session.execute(stmt)
            jobs = result.scalars().all()
            return [self._job_to_dto(j) for j in jobs]

    async def create_rule(
        self, workflow_id: UUID, rule: CreateRuleDTO
    ) -> Optional[int]:
        async with self.async_session() as session:
            # Check workflow exists
            wf_exists = await session.get(Workflow, workflow_id)
            if not wf_exists:
                return None

            new_rule = Rule(
                name=rule.name,
                workflow_id=workflow_id,
                total_job_count=rule.total_job_count,
            )
            session.add(new_rule)
            await session.commit()
            await session.refresh(new_rule)
            return new_rule.id

    async def update_rule(
        self, workflow_id: UUID, rule_id: int, update: UpdateRuleDTO
    ) -> Optional[int]:
        async with self.async_session() as session:
            stmt = select(Rule).where(
                and_(Rule.id == rule_id, Rule.workflow_id == workflow_id)
            )
            result = await session.execute(stmt)
            rule = result.scalar_one_or_none()

            if not rule:
                return None

            if update.total_job_count is not None:
                rule.total_job_count = update.total_job_count
            if update.jobs_finished is not None:
                rule.jobs_finished = update.jobs_finished

            await session.commit()
            await session.refresh(rule)
            return rule_id

    async def create_job(
        self, workflow_id: UUID, rule_id: int, job: CreateJobDTO
    ) -> Optional[JobDTO]:
        async with self.async_session() as session:
            # Verify rule belongs to workflow
            stmt = select(Rule).where(
                and_(Rule.id == rule_id, Rule.workflow_id == workflow_id)
            )
            result = await session.execute(stmt)
            if not result.scalar_one_or_none():
                return None

            new_job = Job(
                snakemake_id=job.snakemake_id,
                workflow_id=workflow_id,
                rule_id=rule_id,
                status=job.status,
                threads=job.threads,
                started_at=job.started_at,
                message=job.message,
                wildcards=job.wildcards,
                reason=job.reason,
                resources=job.resources,
                shellcmd=job.shellcmd,
                priority=job.priority,
                group_id=job.group_id,
            )
            session.add(new_job)
            await session.commit()
            await session.refresh(new_job, ["files"])
            return self._job_to_dto(new_job)

    async def get_job(self, workflow_id: UUID, job_id: int) -> Optional[JobDTO]:
        async with self.async_session() as session:
            stmt = (
                select(Job)
                .options(selectinload(Job.files))
                .where(and_(Job.id == job_id, Job.workflow_id == workflow_id))
            )
            result = await session.execute(stmt)
            job = result.scalar_one_or_none()
            return self._job_to_dto(job) if job else None

    async def update_job(
        self, workflow_id: UUID, rule_id: int, job_id: int, update: UpdateJobDTO
    ) -> Optional[JobDTO]:
        async with self.async_session() as session:
            stmt = (
                select(Job)
                .options(selectinload(Job.files))
                .where(
                    and_(
                        Job.id == job_id,
                        Job.workflow_id == workflow_id,
                        Job.rule_id == rule_id,
                    )
                )
            )
            result = await session.execute(stmt)
            job = result.scalar_one_or_none()

            if not job:
                return None

            if update.status is not None:
                job.status = update.status
            if update.end_time is not None:
                job.end_time = update.end_time

            await session.commit()
            await session.refresh(job)
            return self._job_to_dto(job)

    async def create_file(
        self, workflow_id: UUID, job_id: int, file: CreateFileDTO
    ) -> Optional[FileDTO]:
        async with self.async_session() as session:
            # Verify job belongs to workflow
            stmt = select(Job).where(
                and_(Job.id == job_id, Job.workflow_id == workflow_id)
            )
            result = await session.execute(stmt)
            if not result.scalar_one_or_none():
                return None

            new_file = File(job_id=job_id, path=file.path, file_type=file.file_type)
            session.add(new_file)
            await session.commit()
            await session.refresh(new_file)
            return self._file_to_dto(new_file)

    def _workflow_to_dto(self, workflow: Workflow) -> WorkflowDTO:
        return WorkflowDTO(
            id=workflow.id,
            status=workflow.status,
            name=workflow.snakefile or "unnamed",
            total_job_count=workflow.total_job_count,
            jobs_finished=workflow.jobs_finished,
            started_at=workflow.started_at,
            updated_at=workflow.updated_at,
            snakefile=workflow.snakefile,
            end_time=workflow.end_time,
            dryrun=workflow.dryrun,
            command_line=workflow.command_line,
            rule_ids=[r.id for r in workflow.rules]
            if hasattr(workflow, "rules") and workflow.rules
            else [],
        )

    def _rule_to_dto(self, rule: Rule, job_counts: JobCounts) -> RuleDTO:
        return RuleDTO(
            id=rule.id,
            name=rule.name,
            workflow_id=rule.workflow_id,
            total_job_count=rule.total_job_count,
            jobs_finished=rule.jobs_finished,
            updated_at=rule.updated_at,
            job_counts=job_counts,
        )

    def _job_to_dto(self, job: Job) -> JobDTO:
        return JobDTO(
            id=job.id,
            snakemake_id=job.snakemake_id,
            workflow_id=job.workflow_id,
            rule_id=job.rule_id,
            status=job.status,
            threads=job.threads,
            started_at=job.started_at,
            message=job.message,
            wildcards=job.wildcards,
            reason=job.reason,
            resources=job.resources,
            shellcmd=job.shellcmd,
            priority=job.priority,
            end_time=job.end_time,
            group_id=job.group_id,
            files=[self._file_to_dto(f) for f in job.files],
        )

    async def list_jobs(
        self,
        workflow_id: UUID,
        status: Optional[Status] = None,
        rule_id: Optional[int] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        order_by: str = "end_time",
        descending: bool = True,
    ) -> List[JobDTO]:
        async with self.async_session() as session:
            stmt = (
                select(Job)
                .options(selectinload(Job.rule))
                .options(selectinload(Job.files))
                .where(Job.workflow_id == workflow_id)
            )

            if status:
                stmt = stmt.where(Job.status == status)

            if rule_id:
                stmt = stmt.where(Job.rule_id == rule_id)

            if since:
                stmt = stmt.where(Job.end_time >= since)

            order_column = getattr(Job, order_by, Job.end_time)
            stmt = stmt.order_by(order_column.desc() if descending else order_column)

            if limit:
                stmt = stmt.limit(limit)
            if offset:
                stmt = stmt.offset(offset)

            result = await session.execute(stmt)
            jobs = result.scalars().all()

            # Convert to DTOs with rule names
            job_dtos = []
            for job in jobs:
                # Now job.rule is properly loaded
                rule_name = job.rule.name if job.rule else None
                job_dto = self._job_to_dto(job)
                job_dto.rule_name = rule_name
                job_dtos.append(job_dto)

            return job_dtos

    def _file_to_dto(self, file: File) -> FileDTO:
        return FileDTO(
            id=file.id, job_id=file.job_id, path=file.path, file_type=file.file_type
        )

    async def prune(
        self,
        before_date: Optional[datetime] = None,
        status: Optional[Status] = None,
    ) -> int:
        async with self.async_session() as session:
            stmt = select(Workflow)
            if before_date:
                stmt = stmt.where(Workflow.started_at < before_date)
            if status:
                stmt = stmt.where(Workflow.status == status)

            result = await session.execute(stmt)
            workflows = result.scalars().all()

            deleted_count = 0
            for workflow in workflows:
                await session.delete(workflow)
                deleted_count += 1

            if deleted_count > 0:
                await session.commit()

            return deleted_count
