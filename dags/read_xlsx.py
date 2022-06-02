import pandas as pd
import numpy as np
import datetime
import openpyxl
import os

from datetime import timedelta, date
from dateutil import rrule
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from connections.oracle.connections_sml import connect_ugo, connect_hdata, engine_ugo, connect
from connections.oracle.connections import connect_ugo, connect_hdata, engine_ugo, connect
from collections import OrderedDict as od
from queries.unimed_go.queries import *
from queries.unimed_go.queries_hdata import *

from utils.integrity_checker import notify_email

print("Path of the file..", os.path.abspath('ATENDIMENTO_PACIENTE.xlsx'))
print("Entrou no df_atendimento_paciente")

print(dt_ini.strftime('%d/%m/%Y'), ' a ', dt_ontem.strftime('%d/%m/%Y'))

df_dim = pd.read_sql(query_atendimento_paciente.format(data_ini=dt_ini.strftime('%d/%m/%Y'), data_fim=dt_ontem.strftime('%d/%m/%Y')), connect_ugo())
print(df_dim.info())

df = pd.DataFrame([[11, 21, 31], [12, 22, 32], [31, 32, 33]], index=['one', 'two', 'three'], columns=['a', 'b', 'c'])

print(df)

df.to_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')

df = pd.read_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')
print(df)