import pandas as pd

# Define file paths and names
filepath = 'C:/Users/FaizulBinMdNor/Downloads/'
filename = 'Template15605.csv'

# Define the resampling frequency as '1min'
freq = '1min'


bold = '\033[1m'
Blue = '\033[34m'
Magenta = '\033[35m'
Cyan = '\033[36m'

def fill_missing(df):
    prev_index = None
    a = 0
    for i, r in df.iterrows():
        if pd.isna(r['ETLInsertLogID']):
            a += 1
            if prev_index is not None:
                df.at[i, 'ETLInsertLogID'] = df.at[prev_index, 'ETLInsertLogID']
                df.at[i, 'ETLInsertTimestamp'] = df.at[prev_index, 'ETLInsertTimestamp']
                df.at[i, 'AssetID'] = df.at[prev_index, 'AssetID']
                df.at[i, 'SiteAssetID'] = df.at[prev_index, 'SiteAssetID']
        else:
            prev_index = i
    print(f'{Cyan}Number of missing line: {bold}{Magenta}{a}')
    return df


# Read the CSV file and convert 'UTCReadTime' column to datetime format
df = pd.read_csv(f'{filepath}{filename}')
df['UTCReadTime'] = pd.to_datetime(df['UTCReadTime'], format='%m/%d/%Y %I:%M:%S %p')
df['ReadTime'] = pd.to_datetime(df['ReadTime'], format='%m/%d/%Y %I:%M:%S %p')

readtime_max = df['ReadTime'].max()
readtime_min = df['ReadTime'].min()
readtime_new = pd.date_range(readtime_min, readtime_max, freq=freq)
print(f'{bold}{Magenta}ReadTime start: {Cyan}{readtime_min}\n{Magenta}ReadTime End: {Cyan}{readtime_max}')

utcreadtime_min = df['UTCReadTime'].min()
utcreadtime_max = df['UTCReadTime'].max()
utcreadtime_new = pd.date_range(utcreadtime_min, utcreadtime_max, freq=freq)

new_df = pd.DataFrame({'UTCReadTime': utcreadtime_new})

print(f'{Blue}From date (UTCReadTime): {Magenta}{utcreadtime_min}\n{Blue}To date(UTCReadTime): {Magenta}{utcreadtime_max}')
print(f'{Blue}Number of line before : {bold}{Magenta}{len(df)}')

merge_df = pd.merge(df, new_df, on='UTCReadTime', how='right' )
duplicate_readtime = merge_df[merge_df.duplicated(subset='ReadTime')]

# Remove duplicates based on 'ReadTime'
merge_df.drop_duplicates(subset=['ReadTime'], keep='last', inplace=True, ignore_index=False)

# Reset the index of the DataFrame
merge_df.reset_index(drop=True, inplace=True)

merge_df['ReadTime'] = readtime_new

# Apply the 'fill_missing' function
df_resampled = fill_missing(merge_df)

# Reset the index of the DataFrame
df_resampled.reset_index(drop=True, inplace=True)

# Convert 'UTCReadTime' to a string format
df_resampled['UTCReadTime'] = df_resampled['UTCReadTime'].dt.strftime('%m/%d/%Y %I:%M:%S %p')
df_resampled['ReadTime'] = df_resampled['ReadTime'].dt.strftime('%m/%d/%Y %I:%M:%S %p')

print(f'{Cyan}Number of line after: {bold}{Magenta}{len(df_resampled.index)}')

df_resampled = df_resampled.astype({'AssetID':'int', 'SiteAssetID': 'int'})

# Save the updated DataFrame to a new CSV file
df_resampled.to_csv(f'{filepath}missing_timestamp_updated_{filename}', index=False)
