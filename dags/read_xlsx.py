import os
import pandas as pd

print("Path of the file..", os.path.abspath('ATENDIMENTO_PACIENTE.xlsx'))

df = pd.read_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')
print(df)