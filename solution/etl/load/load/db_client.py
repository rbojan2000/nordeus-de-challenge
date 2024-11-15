from load.constants import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_DRIVER, DB_URL
from pyspark.sql import DataFrame

class DBClient():
    def __init__(self):
        self.host = DB_HOST
        self.port = DB_PORT
        self.name = DB_NAME
        self.user = DB_USER
        self.password = DB_PASSWORD
        self.driver = DB_DRIVER
        self.url = DB_URL

    def write(self, dataset: DataFrame, table_name: str) -> None:
        properties = {
            "user": self.user,
            "password": self.password,
            "driver": self.driver
        }
        dataset.write.jdbc(DB_URL, table_name, mode="overwrite", properties=properties)
