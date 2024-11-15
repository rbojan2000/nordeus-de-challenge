import pandas as pd
from nordeus_cli.constants import DB_URL
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


class DBClient:
    def __init__(self) -> None:
        self.connection_string: str = DB_URL
        self.engine: Engine = create_engine(self.connection_string)

    def submit_query(self, query: str) -> pd.DataFrame:
        df = pd.read_sql_query(query, self.engine)
        return df
