import sqlalchemy as db
from dagster._utils.backcompat import experimental_class_warning

import dagster._check as check
from dagster._core.storage.schedules import ScheduleStorageSqlMetadata, SqlScheduleStorage
from dagster._core.storage.sql import (
    check_alembic_revision,
    create_engine,
    run_alembic_upgrade,
    stamp_alembic_rev,
)
from dagster._serdes import ConfigurableClass, ConfigurableClassData

from ..utils import (
    create_mssql_connection,
    mssql_alembic_config,
    mssql_config,
    mssql_url_from_config,
    retry_mssql_connection_fn,
    retry_mssql_creation_fn,
)


class MSSQLScheduleStorage(SqlScheduleStorage, ConfigurableClass):
    """MSSQL-backed run storage.

    Users should not directly instantiate this class; it is instantiated by internal machinery when
    ``dagit`` and ``dagster-graphql`` load, based on the values in the ``dagster.yaml`` file in
    ``$DAGSTER_HOME``. Configuration of this class should be done by setting values in that file.

    .. literalinclude:: ../../../../../../examples/docs_snippets/docs_snippets/deploying/dagster-mssql.yaml
       :caption: dagster.yaml
       :start-after: start_marker_schedules
       :end-before: end_marker_schedules
       :language: YAML

    Note that the fields in this config are :py:class:`~dagster.StringSource` and
    :py:class:`~dagster.IntSource` and can be configured from environment variables.
    """

    def __init__(self, mssql_url, inst_data=None):
        experimental_class_warning("MSSQLScheduleStorage")
        self._inst_data = check.opt_inst_param(inst_data, "inst_data", ConfigurableClassData)
        self.mssql_url = mssql_url

        # Default to not holding any connections open to prevent accumulating connections per DagsterInstance
        self._engine = create_engine(
            self.mssql_url,
            isolation_level="AUTOCOMMIT",
            poolclass=db.pool.NullPool,
        )

        # Stamp and create tables if the main table does not exist (we can't check alembic
        # revision because alembic config may be shared with other storage classes)
        table_names = retry_mssql_connection_fn(db.inspect(self._engine).get_table_names)
        if "jobs" not in table_names:
            retry_mssql_creation_fn(self._init_db)

        super().__init__()

    def _init_db(self):
        with self.connect() as conn:
            with conn.begin():
                ScheduleStorageSqlMetadata.create_all(conn)
                stamp_alembic_rev(mssql_alembic_config(__file__), conn)

    def optimize_for_dagit(self, statement_timeout, pool_recycle):
        # When running in dagit, hold an open connection
        # https://github.com/dagster-io/dagster/issues/3719
        self._engine = create_engine(
            self.mssql_url,
            isolation_level="AUTOCOMMIT",
            pool_size=1,
            pool_recycle=pool_recycle,
        )

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return mssql_config()

    @staticmethod
    def from_config_value(inst_data, config_value):
        return MSSQLScheduleStorage(
            inst_data=inst_data, mssql_url=mssql_url_from_config(config_value)
        )

    @staticmethod
    def wipe_storage(mssql_url):
        engine = create_engine(mssql_url, isolation_level="AUTOCOMMIT", poolclass=db.pool.NullPool)
        try:
            ScheduleStorageSqlMetadata.drop_all(engine)
        finally:
            engine.dispose()

    @staticmethod
    def create_clean_storage(mssql_url):
        MSSQLScheduleStorage.wipe_storage(mssql_url)
        return MSSQLScheduleStorage(mssql_url)

    def connect(self, run_id=None):  # pylint: disable=arguments-differ, unused-argument
        return create_mssql_connection(self._engine, __file__, "schedule")

    def upgrade(self):
        alembic_config = mssql_alembic_config(__file__)
        run_alembic_upgrade(alembic_config, self._engine)

    @property
    def supports_batch_queries(self):
        return False

    def alembic_version(self):
        alembic_config = mssql_alembic_config(__file__)
        with self.connect() as conn:
            return check_alembic_revision(alembic_config, conn)