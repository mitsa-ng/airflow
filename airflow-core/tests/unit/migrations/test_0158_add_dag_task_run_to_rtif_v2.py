#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from importlib import import_module
from uuid import UUID

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.exc import IntegrityError

from airflow import settings

pytestmark = pytest.mark.db_test

migration = import_module("airflow.migrations.versions.0158_3_5_0_add_dag_task_run_to_rtif_v2")

TI_ID = UUID(int=1)
RTIF_ID = UUID(int=2)
UNRELATED_TI_ID = UUID(int=3)

metadata = sa.MetaData()
task_instance = sa.Table(
    "task_instance",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("dag_id", sa.String(250)),
    sa.Column("task_id", sa.String(250)),
    sa.Column("run_id", sa.String(250)),
)
rtif_v2 = sa.Table(
    "rtif_v2",
    metadata,
    sa.Column("id", sa.Uuid, primary_key=True),
    sa.Column("task_instance_id", sa.Uuid, sa.ForeignKey("task_instance.id", ondelete="CASCADE")),
    sa.Column("rendered_fields", sa.JSON),
)
# The columns only exist after upgrade(), so query them through a lightweight table.
rtif_v2_migrated = sa.table(
    "rtif_v2",
    sa.column("id"),
    sa.column("task_instance_id"),
    sa.column("rendered_fields"),
    sa.column("dag_id"),
    sa.column("task_id"),
    sa.column("run_id"),
)


def _run_migration(engine, migrate):
    with engine.begin() as connection, Operations.context(MigrationContext.configure(connection)):
        migrate()


@pytest.fixture
def migrated_database(tmp_path):
    source = settings.engine
    name = f"rtif_coordinates_{UNRELATED_TI_ID.hex}"
    if source.dialect.name == "sqlite":
        url = source.url.set(database=str(tmp_path / "rtif_coordinates.db"))
    else:
        url = source.url.set(database=name)
        with source.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
            admin.exec_driver_sql(f"CREATE DATABASE {name}")
    engine = sa.create_engine(url)
    try:
        metadata.create_all(engine)
        with engine.begin() as connection:
            connection.execute(
                task_instance.insert().values(
                    id=TI_ID, dag_id="dag", task_id="task", run_id="scheduled__2026-11-23"
                )
            )
            connection.execute(
                rtif_v2.insert().values(id=RTIF_ID, task_instance_id=TI_ID, rendered_fields={"a": 1})
            )
        yield engine
    finally:
        engine.dispose()
        if source.dialect.name != "sqlite":
            with source.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
                admin.exec_driver_sql(f"DROP DATABASE {name}")


def test_upgrade_backfills_coordinates_and_enforces_not_null(migrated_database):
    _run_migration(migrated_database, migration.upgrade)

    with migrated_database.connect() as connection:
        row = connection.execute(
            sa.select(rtif_v2_migrated.c.dag_id, rtif_v2_migrated.c.task_id, rtif_v2_migrated.c.run_id)
        ).one()
        assert tuple(row) == ("dag", "task", "scheduled__2026-11-23")
        assert any(
            index["name"] == "rtif_v2_dag_task_run"
            for index in sa.inspect(migrated_database).get_indexes("rtif_v2")
        )
        with pytest.raises(IntegrityError):
            connection.execute(
                rtif_v2_migrated.insert().values(id=str(UNRELATED_TI_ID), task_instance_id=str(TI_ID))
            )


def test_downgrade_drops_coordinates_and_index(migrated_database):
    _run_migration(migrated_database, migration.upgrade)
    _run_migration(migrated_database, migration.downgrade)

    columns = {column["name"] for column in sa.inspect(migrated_database).get_columns("rtif_v2")}
    assert not columns & {"dag_id", "task_id", "run_id"}
    assert all(
        index["name"] != "rtif_v2_dag_task_run"
        for index in sa.inspect(migrated_database).get_indexes("rtif_v2")
    )
