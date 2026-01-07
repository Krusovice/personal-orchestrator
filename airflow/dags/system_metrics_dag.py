from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

@dag(
    schedule='*/1 * * * *',  # Run every 1 minute
    start_date=datetime(2025, 2, 21),
    catchup=False,
    tags=["system_metrics_monitoring"],
)
def monitor_system_metrics():

    @task
    def check_if_table_exists():
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        create_table_query = """
        CREATE TABLE IF NOT EXISTS system_metrics_table (
            id SERIAL PRIMARY KEY,
            timestamp timestamp NOT NULL,
            cpu_usage REAL,
            ram_usage REAL
        );
        """

        hook.run(create_table_query)


    @task()
    def check_new_data():
        """Check if new data has been inserted in the last 5 minutes"""
        hook = PostgresHook(postgres_conn_id="postgres_default")
        result = hook.get_first("""
            SELECT COUNT(*) FROM system_metrics_table
            WHERE timestamp > NOW() - INTERVAL '5 minutes'
        """)
        if result and result[0] > 0:
            return "New data observed"
        else:
            return "No new data observed"

    check_if_table_exists() >> check_new_data()
monitor_system_metrics()