import pandas as pd
# Define file paths and names
filepath = 'C:/Users/FaizulBinMdNor/Downloads/'
filename = 'Template12924.csv'

data = pd.read_csv(filepath+filename)
dup_readtime = data[data.duplicated('ReadTime', keep=False)]
dup_UTCReadTime = data[data.duplicated('UTCReadTime', keep=False)]

data.drop_duplicates(subset='ReadTime', keep='first', inplace=True)
data.drop_duplicates(subset='UTCReadTime', keep='first', inplace=True)

if not (dup_readtime.empty) & (dup_UTCReadTime.empty):
    print(dup_readtime)
    print(dup_UTCReadTime)
    data.to_csv(f'{filepath}remove_dup_{filename}', index=False)

elif not (dup_UTCReadTime.empty):
    print(dup_UTCReadTime)
    print('No duplicate - ReadTime')

elif not (dup_readtime.empty):
    print(dup_readtime)
    print('No duplicate - UTCReadTime')
else:
    print('No duplicate - UTCReadTime')
    print('No duplicate - ReadTime')



