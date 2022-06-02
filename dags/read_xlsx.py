import pandas as pd

df = pd.read_table(r'~/unimedgo_hdata-airflow/dags/ATENDIMENTO_PACIENTE.txt')
print(df)