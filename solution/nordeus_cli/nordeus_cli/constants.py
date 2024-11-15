DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "nordeus_db"
DB_USER = "nordeus_user"
DB_PASSWORD = "nordeus"
DB_DRIVER = "org.postgresql.Driver"

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

USER_SESSION_STATS_JOB_TABLE_NAME = "user_session_stats"
MATCH_STATS_JOB_TABLE_NAME = "match_stats"
