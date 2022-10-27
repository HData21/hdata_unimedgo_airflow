import airflow
import unidecode
import pandas as pd
import numpy as np
import datetime

from datetime import timedelta, date
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from connections.oracle.connections_sml import connect_ugo, connect_hdata, engine_ugo, connect
from connections.oracle.connections import connect_ugo, connect_hdata, engine_ugo, connect
from collections import OrderedDict as od
from queries.unimed_go.queries import *
from queries.unimed_go.queries_hdata import *
from queries.unimed_go.queries_complementar import *

from utils.integrity_checker import notify_email

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

HOSPITAL = "UNIMED GO"

def update_cells(df_eq, table_name, CD):
    d = df_eq.to_dict(orient='split')
    print(d)
    for dado in d['data']:
        for i in range(len(dado) - 1):
            conn = connect_hdata()
            cursor = conn.cursor()

            query = ''
            query = 'UPDATE {nome_tabela} '.format(nome_tabela=table_name)
            if pd.isna(dado[i + 1]):
                query += 'SET {nome_coluna} is null '.format(nome_coluna=d['columns'][i + 1])
            else:
                if type(dado[i + 1]) == np.int64 or type(dado[i + 1]) == np.float64:
                    query += 'SET {nome_coluna} = {novo_valor} '.format(nome_coluna=d['columns'][i + 1],
                                                            novo_valor=dado[i + 1])
                else:
                    query += 'SET {nome_coluna} = \'{novo_valor}\' '.format(nome_coluna=d['columns'][i + 1],
                                                            novo_valor=dado[i + 1])
            query += 'WHERE {cd} IN({todos_cds})'.format(cd=CD, todos_cds=dado[0])

            # print(query)
            cursor.execute(query)
            conn.commit()
            conn.close()

def novos_campos():
    print("Novos campos serão atualizados")
    dt_inicio = datetime.datetime(2022,1,1)
    for dt in rrule.rrule(rrule.DAILY, dtstart=dt_inicio, until=dt_ontem):
        #query para trazer cd e novos campos
        df_dim = pd.read_sql(query_cancelamento_seq_classif.format(data_ini=dt.strftime('%d/%m/%Y'), 
                                                data_fim=dt.strftime('%d/%m/%Y')), connect_ugo())
        if not df_dim.empty:
            update_cells(df_eq=df_dim,table_name='ATENDIMENTO_PACIENTE',CD='NR_ATENDIMENTO')
    print("Atualização finalizada!")

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_ini = dt_ontem - datetime.timedelta(days=5)

dag = DAG("captura_dados_complementar_unimed_go", default_args=default_args, schedule_interval=None)

t0 = PythonOperator(
    task_id="captura_novos_campos_hugyn",
    python_callable=novos_campos,
    on_failure_callback=notify_email,
    dag=dag)

t0