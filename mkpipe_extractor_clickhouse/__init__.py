import os
from typing import Optional

from mkpipe.spark.base import BaseExtractor
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

JAR_PACKAGES = [
    'com.clickhouse.spark:clickhouse-spark-runtime-4.0_2.13:0.8.1',
    'com.clickhouse:clickhouse-http-client:0.7.2',
    'org.apache.httpcomponents.client5:httpclient5:5.3.1',
]

logger = get_logger(__name__)


class ClickhouseExtractor(BaseExtractor, variant='clickhouse'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.host = connection.host
        self.port = connection.port or 8123
        self.username = connection.user or 'default'
        self.password = str(connection.password or '')
        self.database = connection.database

    def _base_options(self) -> dict:
        return {
            'host': self.host,
            'port': str(self.port),
            'user': self.username,
            'password': self.password,
            'database': self.database,
        }

    def _build_reader(self, spark, table_or_query: str, is_query: bool = False):
        reader = spark.read.format('clickhouse')
        for k, v in self._base_options().items():
            reader = reader.option(k, v)
        if is_query:
            reader = reader.option('query', table_or_query)
        else:
            reader = reader.option('dbtable', table_or_query)
        return reader.load()

    def _resolve_custom_query(self, table: TableConfig) -> Optional[str]:
        if table.custom_query:
            return table.custom_query
        if table.custom_query_file:
            path = os.path.abspath(os.path.join(os.getcwd(), 'sql', table.custom_query_file))
            with open(path) as f:
                return f.read()
        return None

    def extract(self, table: TableConfig, spark, last_point: Optional[str] = None) -> ExtractResult:
        logger.info({
            'table': table.target_name,
            'status': 'extracting',
            'replication_method': table.replication_method.value,
        })

        custom_query = self._resolve_custom_query(table)

        if table.replication_method.value == 'incremental' and table.iterate_column:
            if last_point:
                write_mode = 'append'
                if table.iterate_column_type == 'int':
                    filter_clause = f'WHERE {table.iterate_column} >= {last_point}'
                else:
                    filter_clause = f"WHERE {table.iterate_column} >= '{last_point}'"
            else:
                write_mode = 'overwrite'
                filter_clause = 'WHERE 1=1'

            if custom_query:
                sql = custom_query.replace('{query_filter}', filter_clause)
            else:
                sql = f'SELECT * FROM {table.name} {filter_clause}'

            df = self._build_reader(spark, sql, is_query=True)

            from pyspark.sql import functions as F
            row = df.agg(F.max(table.iterate_column).alias('max_val')).first()
            last_point_value = str(row['max_val']) if row and row['max_val'] is not None else None
        else:
            write_mode = 'overwrite'
            if custom_query:
                sql = custom_query.replace('{query_filter}', 'WHERE 1=1')
                df = self._build_reader(spark, sql, is_query=True)
            else:
                df = self._build_reader(spark, table.name)
            last_point_value = None

        logger.info({'table': table.target_name, 'status': 'extracted', 'write_mode': write_mode})
        return ExtractResult(df=df, write_mode=write_mode, last_point_value=last_point_value)
