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

os.environ["NLS_LANG"] = ".UTF8"
dsn_tns = cx_Oracle.makedsn('10.64.25.41', 15120, service_name='dbtasy')
connect_ugo = cx_Oracle.connect('HDATA', 'HDATATS2022', dsn_tns)

print("Entrou no df_atendimento_paciente")
dt_ini = datetime.datetime(2021,12,23)
dt_ontem = datetime.datetime(2021,12,23)
print(dt_ini.strftime('%d/%m/%Y'), ' a ', dt_ontem.strftime('%d/%m/%Y'))
df_dim = pd.read_sql(query_atendimento_paciente.format(data_ini=dt_ini.strftime('%d/%m/%Y'), data_fim=dt_ontem.strftime('%d/%m/%Y')), connect_ugo)
print(df_dim.info())

df = pd.DataFrame([[11, 21, 31], [12, 22, 32], [31, 32, 33]], index=['one', 'two', 'three'], columns=['a', 'b', 'c'])

print(df)

df.to_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')

df = pd.read_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')
print(df)