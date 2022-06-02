import os
import pandas as pd

print("Path of the file..", os.path.abspath('ATENDIMENTO_PACIENTE.txt'))

df = pd.read_fwf('/home/raphael.hdata/hdata_unimedgo_airflow/')
print(df)