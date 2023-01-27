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

START_DATE = datetime.datetime(2023,1,25)

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
    cols = df_eq.dtypes[df_eq.dtypes=='datetime64[ns]'].index
    d = df_eq.to_dict(orient='split')
    #print(d)
    for dado in d['data']:
        for i in range(len(dado) - 1):
            conn = connect_hdata()
            cursor = conn.cursor()

            query = ''
            query = 'UPDATE {nome_tabela} '.format(nome_tabela=table_name)
            if pd.isna(dado[i + 1]):
                query += 'SET {nome_coluna} = null '.format(nome_coluna=d['columns'][i + 1])
            else:
                #print(type(dado[i + 1]))
                if type(dado[i + 1]) == np.int64 or type(dado[i + 1]) == np.float64 or type(dado[i + 1]) == int:
                    query += 'SET {nome_coluna} = {novo_valor} '.format(nome_coluna=d['columns'][i + 1],
                                                            novo_valor=dado[i + 1])
                elif d['columns'][i + 1] in cols:
                    query += 'SET {nome_coluna} = TIMESTAMP \'{novo_valor}\' '.format(nome_coluna=d['columns'][i + 1],
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
        print("Atualizando:..."+ str(dt))
        #query para trazer cd e novos campos
        df_dim = pd.read_sql(query_seq_classif.format(data_ini=dt.strftime('%d/%m/%Y'), 
                                                data_fim=dt.strftime('%d/%m/%Y')), connect_ugo())
        if not df_dim.empty:
            df_dim = df_dim.dropna(subset=['NR_SEQ_CLASSIFICACAO'])
            update_cells(df_eq=df_dim,table_name='ATENDIMENTO_PACIENTE',CD='NR_ATENDIMENTO')
    print("Atualização finalizada!")

dt_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
dt_ini = dt_ontem - datetime.timedelta(days=5)

dag = DAG("captura_dados_complementar_unimed", default_args=default_args, schedule_interval='45 7 * * *')

t0 = PythonOperator(
    task_id="captura_novos_campos",
    python_callable=novos_campos,
    on_failure_callback=notify_email,
    dag=dag)

t0