import pandas as pd
from datetime import datetime

now = datetime.now()
n = now.strftime('%Y%m%d%H%M')
filepath = 'C:/Users/FaizulBinMdNor/Downloads/'
filename = 'Template13154'
input_file = f'{filepath}{filename}.csv'
output_file = f'{input_file}_{n}.csv'

data = pd.read_csv(input_file)
duplicate_readtime = data[data.duplicated(subset=['ReadTime'])]
duplicate_utcreadtime = data[data.duplicated(subset=['UTCReadTime'])]