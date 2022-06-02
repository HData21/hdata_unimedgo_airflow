import pandas as pd

df = pd.read_fwf('~/unimedgo_hdata-airflow/dags/ATENDIMENTO_PACIENTE.txt')
print(df)