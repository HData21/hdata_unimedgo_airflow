import pandas as pd
import numpy as np
import datetime
import openpyxl
import os
import cx_Oracle

from datetime import timedelta, date
from dateutil import rrule

print("Path of the file..", os.path.abspath('ATENDIMENTO_PACIENTE.xlsx'))

query_atendimento_paciente = "SELECT * FROM TASY.VW_HDATA_ATENDIMENTO_PACIENTE WHERE DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY') AND DT_ENTRADA < TO_DATE('{data_fim}', 'DD/MM/YYYY') + INTERVAL '1' DAY"

cx_Oracle.init_oracle_client(lib_dir="/opt/oracle/instant_client_19_13")
connect_ugo = cx_Oracle.connect(user="hdata", password="hdatats2022", dsn="10.64.25.41:15120/dbtasy")

print("Entrou no df_atendimento_paciente")
print(dt_ini.strftime('%d/%m/%Y'), ' a ', dt_ontem.strftime('%d/%m/%Y'))
df_dim = pd.read_sql(query_atendimento_paciente.format(data_ini=dt_ini.strftime('%d/%m/%Y'), data_fim=dt_ontem.strftime('%d/%m/%Y')), conn())
print(df_dim.info())

df = pd.DataFrame([[11, 21, 31], [12, 22, 32], [31, 32, 33]], index=['one', 'two', 'three'], columns=['a', 'b', 'c'])

print(df)

df.to_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')

df = pd.read_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')
print(df)