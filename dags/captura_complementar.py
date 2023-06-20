import airflow
import unidecode
import pandas as pd
import numpy as np
import datetime

from datetime import timedelta, date
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from collections import OrderedDict as od
from queries.unimed_go.queries import *
from queries.unimed_go.queries_hdata import *
from queries.unimed_go.queries_complementar import *

from utils.upsert_default import by_date_upsert
from utils.integrity_checker import notify_email
from utils.config import STAGE_NAMESPACE

START_DATE = airflow.utils.dates.days_ago(1)

default_args = {
    "owner": "lucas",
    "depends_on_past": False,
    "start_date": START_DATE,
    "email": ["lucas.freire@hdata.med.br"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=0),
    "provide_context": True,
}

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_ini = datetime.datetime.today() - datetime.timedelta(days=15)
# dt_ini = datetime.datetime(2023,5,1)

dag = DAG("captura_dados_complementar_unimed", default_args=default_args, schedule_interval=None)

t0 = PythonOperator(
    task_id="upsert_evolucao_paciente",
    python_callable=by_date_upsert,
    op_kwargs={
        'query_origem': query_evolucao,
        'tabela_destino': 'EVOLUCAO_PACIENTE',
        'pk' : 'CD_EVOLUCAO',
        'inicio' : dt_ini,
        'fim' : dt_ontem
    },
    on_failure_callback=notify_email,
    dag=dag)

t0