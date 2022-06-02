import os
import pandas as pd

print("Path of the file..", os.path.abspath('ATENDIMENTO_PACIENTE.xlsx'))

df = pd.DataFrame([[11, 21, 31], [12, 22, 32], [31, 32, 33]], index=['one', 'two', 'three'], columns=['a', 'b', 'c'])

print(df)

df.to_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')

df = pd.read_excel('/home/raphael.hdata/hdata_unimedgo_airflow/dags/ATENDIMENTO_PACIENTE.xlsx')
print(df)