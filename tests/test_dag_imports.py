import os
from airflow.models import DagBag


def test_dag_imports():
    dag_folder = os.path.join(os.getcwd(), "dags")
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)

    assert len(dag_bag.import_errors) == 0, dag_bag.import_errors