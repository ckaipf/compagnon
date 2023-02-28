def get_postgres_uri():
    host = "localhost"
    port = 5432
    password = "postgres"
    user, db_name = "postgres", "loop"
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
