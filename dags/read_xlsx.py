import pandas as pd
import numpy as np
import datetime
import openpyxl
import os
import cx_Oracle

from datetime import timedelta, date
from dateutil import rrule

print("Path of the file..", os.path.abspath('ATENDIMENTO_PACIENTE.xlsx'))

query_atendimento_paciente = "SELECT * FROM TASY.VW_HDATA_ATENDIMENTO_PACIENTE WHERE DT_ENTRADA >= TO_DATE('{data_ini}', 'DD/MM/YYYY HH24:MI:SS') AND DT_ENTRADA <= TO_DATE('{data_fim}', 'DD/MM/YYYY HH24:MI:SS')"

os.environ["NLS_LANG"] = ".UTF8"
dsn_tns = cx_Oracle.makedsn('10.64.25.41', 15120, service_name='dbtasy')
connect_ugo = cx_Oracle.connect('HDATA', 'HDATATS2022', dsn_tns)

print("Entrou no df_atendimento_paciente")
dt_ini = datetime.datetime(2021,12,23, 10, 0, 0)
dt_ontem = datetime.datetime(2021,12,23, 14, 0, 0)
print(dt_ini.strftime('%d/%m/%Y %H:%M:%S'), ' a ', dt_ontem.strftime('%d/%m/%Y %H:%M:%S'))
df_dim = pd.read_sql(query_atendimento_paciente.format(data_ini=dt_ini.strftime('%d/%m/%Y'), data_fim=dt_ontem.strftime('%d/%m/%Y')), connect_ugo)
print(df_dim.info())

compression_opts = dict(method='zip', archive_name='ATENDIMENTO_PACIENTE.csv')
df_dim.to_csv('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.zip', index=False, compression=compression_opts)

print("DONE")

# df = pd.DataFrame([[11, 21, 31], [12, 22, 32], [31, 32, 33]], index=['one', 'two', 'three'], columns=['a', 'b', 'c'])

# print(df)

# df.to_csv('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.csv')

# df = pd.read_csv('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.csv')
# print(df)