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
"""
Add dag_id, task_id and run_id columns to rtif_v2 and index them.

The rendered-fields cleanup filters rtif_v2 through these columns instead of
probing the task_instance history once per attempt, whose cost grew with every
attempt a task ever had (#77237).

Revision ID: 8f2c6a91d4e3
Revises: c64b10719069
Create Date: 2026-11-23 12:00:00.000000
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from airflow.migrations.utils import disable_sqlite_fkeys

revision = "8f2c6a91d4e3"
down_revision = "c64b10719069"
branch_labels = None
depends_on = None
airflow_version = "3.5.0"

_RTIF_COLUMNS = ("dag_id", "task_id", "run_id")


def upgrade():
    """Denormalize the owning task instance coordinates onto rtif_v2 and index them."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("rtif_v2") as batch_op:
            for column in _RTIF_COLUMNS:
                batch_op.add_column(sa.Column(column, sa.String(length=250), nullable=True))

    rtif_v2 = sa.table("rtif_v2", sa.column("task_instance_id"), *(sa.column(c) for c in _RTIF_COLUMNS))
    task_instance = sa.table("task_instance", sa.column("id"), *(sa.column(c) for c in _RTIF_COLUMNS))
    # A task instance's dag_id, task_id and run_id never change after creation, so the
    # copies cannot drift. Correlated subqueries keep one statement per column set
    # portable across PostgreSQL, MySQL and SQLite.
    op.execute(
        sa.update(rtif_v2).values(
            {
                column: sa.select(getattr(task_instance.c, column))
                .where(task_instance.c.id == rtif_v2.c.task_instance_id)
                .scalar_subquery()
                for column in _RTIF_COLUMNS
            }
        )
    )

    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("rtif_v2") as batch_op:
            for column in _RTIF_COLUMNS:
                batch_op.alter_column(column, existing_type=sa.String(length=250), nullable=False)
            batch_op.create_index("rtif_v2_dag_task_run", _RTIF_COLUMNS, unique=False)


def downgrade():
    """Drop the rtif_v2 denormalized coordinate columns and their index."""
    with disable_sqlite_fkeys(op):
        with op.batch_alter_table("rtif_v2") as batch_op:
            batch_op.drop_index("rtif_v2_dag_task_run")
            for column in _RTIF_COLUMNS:
                batch_op.drop_column(column)
