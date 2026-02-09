from mkpipe.spark import JdbcExtractor


class ClickhouseExtractor(JdbcExtractor, variant='clickhouse'):
    driver_name = 'clickhouse'
    driver_jdbc = 'com.clickhouse.jdbc.ClickHouseDriver'

    def build_jdbc_url(self):
        return (
            f'jdbc:{self.driver_name}://{self.host}:{self.port}/{self.database}'
            f'?user={self.username}&password={self.password}'
        )
