from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

def run_processing(game):
    # Your processing code here
    # Make sure to use the game parameter to differentiate each iteration

    # Example: Put your code here
    # ...
    pass

# Define your default arguments, schedule interval, etc.
default_args = {
    'owner': 'your_owner',
    'start_date': datetime(2023, 10, 12),
    'retries': 1,  # Set the number of retries
    'retry_delay': timedelta(minutes=5),  # Set the retry delay
}

# Create a DAG
dag = DAG(
    'your_dag_id',
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule interval
    catchup=False,  # Prevent backfilling
)

# List of games
games = [
    "codmw",
    "valorant",
    "kog",
    "ow",
    "pubg",
    "r6siege",
    "rl",
    "csgo",
    "dota2",
    "fifa",
    "lol"
]

# Define a PythonOperator for each game
for game in games:
    task_id = f'process_{game}'
    task = PythonOperator(
        task_id=task_id,
        python_callable=run_processing,
        op_args=[game],
        provide_context=True,
        dag=dag,
    )

    # Set up task dependencies
    if game != games[0]:
        # Set the dependencies, so each game task waits for the previous one to finish
        dag.set_upstream(task_id, f'process_{games[games.index(game) - 1]}')

if __name__ == "__main__":
    dag.cli()
