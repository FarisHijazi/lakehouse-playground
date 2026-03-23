"""
Solution 01: Hello World DAG
=============================
This is a minimal Airflow DAG that demonstrates the core building blocks:
  - Defining a DAG with schedule and default arguments
  - Using BashOperator to run shell commands
  - Using PythonOperator to run Python functions
  - Using EmptyOperator as a no-op start/end marker
  - Setting task dependencies with the >> operator

Run as a standalone script to verify syntax:
    python solution_01_hello_world.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Default arguments applied to every task in this DAG.
# These can be overridden at the individual task level.
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# ---------------------------------------------------------------------------
# Python callables used by PythonOperator tasks.
# Keeping them as standalone functions makes them easy to unit-test.
# ---------------------------------------------------------------------------
def greet():
    """Simple greeting to prove the PythonOperator works."""
    print("Hello from Airflow! Your first DAG is running.")


def report_context(**context):
    """
    Demonstrate how to access the Airflow task instance context.
    The **context dict contains execution_date, ds, task_instance, etc.
    """
    ds = context["ds"]                           # logical date as YYYY-MM-DD
    dag_id = context["task_instance"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]

    print(f"DAG ID       : {dag_id}")
    print(f"Task ID      : {task_id}")
    print(f"Logical date : {ds}")
    print(f"Run ID       : {run_id}")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="hello_world",
    description="A simple introductory DAG with basic operators and dependencies",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,                  # Don't backfill past dates
    tags=["module-03", "exercise"],
) as dag:

    # -- Tasks ---------------------------------------------------------------

    # EmptyOperator: a no-op task useful as a start/end anchor.
    start = EmptyOperator(task_id="start")

    # BashOperator: runs a shell command. Great for calling scripts, CLI tools,
    # or quick checks.
    print_date = BashOperator(
        task_id="print_date",
        bash_command="echo 'Current date:' && date && echo 'Logical date: {{ ds }}'",
    )

    # PythonOperator: runs a Python callable.
    greet_task = PythonOperator(
        task_id="greet",
        python_callable=greet,
    )

    # PythonOperator with context: setting provide_context is not needed in
    # Airflow 2.x when you use **context in the callable signature.
    context_task = PythonOperator(
        task_id="report_context",
        python_callable=report_context,
    )

    end = EmptyOperator(task_id="end")

    # -- Dependencies --------------------------------------------------------
    # start runs first.
    # print_date and greet run in parallel after start.
    # report_context runs after both finish.
    # end runs last.
    #
    # Visual:
    #               +--> print_date --+
    #   start ---+                    +--> report_context --> end
    #               +--> greet -------+
    # -----------------------------------------------------------------------
    start >> [print_date, greet_task] >> context_task >> end


# ---------------------------------------------------------------------------
# Allow running this file directly to check for import/syntax errors.
#   python solution_01_hello_world.py
# This does NOT execute the tasks; it only verifies the DAG parses correctly.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    dag.test()
