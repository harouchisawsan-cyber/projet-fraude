"""DAG Airflow : rejeu des transactions Sparkov dans Kafka.

Feuille de mission d'Airflow (3 tâches, dans cet ordre) :
    1. check_csv     -> le fichier CSV est-il bien dans data/ ?
    2. check_kafka   -> Kafka répond-il ?
    3. run_producer  -> lancer producer.py (CSV -> topic "transactions")

Lancement : interface Airflow (http://localhost:8081) -> DAG "replay_transactions"
-> bouton ▶ "Trigger DAG w/ config" pour choisir le fichier, le nombre de lignes et la vitesse.
"""
import os
import socket
import time
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Chemins et adresses VUS DEPUIS LE CONTENEUR AIRFLOW (pas depuis votre PC)
DATA_DIR = "/opt/airflow/data"                      # = dossier data/ du projet
PRODUCER = "/opt/airflow/scripts/producer.py"       # = producer.py du projet
KAFKA_HOST, KAFKA_PORT = "kafka", 9092              # Kafka à l'intérieur de Docker


def check_csv(**context):
    """Tâche 1 : vérifier que le fichier CSV existe."""
    path = os.path.join(DATA_DIR, context["params"]["file"])
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Fichier introuvable : {path}. Téléchargez Sparkov depuis Kaggle "
            "(kartik2112/fraud-detection) et placez le CSV dans le dossier data/."
        )
    size_mb = os.path.getsize(path) / 1_000_000
    print(f"OK : {path} ({size_mb:.0f} Mo)")


def check_kafka():
    """Tâche 2 : vérifier que Kafka accepte les connexions (5 essais)."""
    for attempt in range(1, 6):
        try:
            with socket.create_connection((KAFKA_HOST, KAFKA_PORT), timeout=5):
                print(f"OK : Kafka répond sur {KAFKA_HOST}:{KAFKA_PORT}")
                return
        except OSError as err:
            print(f"Essai {attempt}/5 : Kafka ne répond pas encore ({err})")
            time.sleep(5)
    raise ConnectionError(f"Kafka injoignable sur {KAFKA_HOST}:{KAFKA_PORT}")


with DAG(
    dag_id="replay_transactions",
    description="Rejoue le dataset Sparkov dans le topic Kafka 'transactions'",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # pas de planning : lancé à la main (bouton ▶)
    catchup=False,
    tags=["fraude", "kafka"],
    # Réglages modifiables au lancement ("Trigger DAG w/ config")
    params={
        "file": Param("fraudTest.csv", type="string",
                      enum=["fraudTest.csv", "fraudTrain.csv"],
                      description="Fichier du dossier data/ à rejouer"),
        "limit": Param(10000, type="integer", minimum=0,
                       description="Nombre de lignes (0 = tout le fichier)"),
        "rate": Param(100, type="number", minimum=0,
                      description="Transactions par seconde (0 = le plus vite possible)"),
    },
) as dag:

    t1_check_csv = PythonOperator(
        task_id="check_csv",
        python_callable=check_csv,
    )

    t2_check_kafka = PythonOperator(
        task_id="check_kafka",
        python_callable=check_kafka,
    )

    t3_run_producer = BashOperator(
        task_id="run_producer",
        bash_command=(
            f"python {PRODUCER}"
            f" --file {DATA_DIR}/{{{{ params.file }}}}"
            " --limit {{ params.limit }}"
            " --rate {{ params.rate }}"
            f" --bootstrap {KAFKA_HOST}:{KAFKA_PORT}"
            " --topic transactions"
        ),
    )

    # L'ordre des tâches : 1 -> 2 -> 3
    t1_check_csv >> t2_check_kafka >> t3_run_producer
