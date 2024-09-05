import pandas as pd

# Define file paths and names
filepath = 'C:/Users/FaizulBinMdNor/Downloads/'
filename = 'Template13545.csv'

def fill_empty_with_adjacent(lst, direction="above"):
    result = lst.copy()
    i = 0
    while i < len(lst):

        if pd.isna(lst[i]) or (lst[i] < 1000.00 or lst[i] > 1050.00):

            if direction == "above" and i > 0:
                if pd.isna(lst[i-1]) or (lst[i] < 1000.00 or lst[i] > 1050.00):
                    print(f'after: {lst[i]}')
                    result[i] = result[i-1]
                    print(f'after: {lst[i]}')
                else:
                    print(f'after: {lst[i]}')
                    result[i] = lst[i+1]
            elif direction == "below" and i < len(lst)-1:
                print(f'after: {lst[i]}')
                result[i] = lst[i+1]
                print(lst[i])

        i += 1

    return result


# Read the CSV file and convert 'UTCReadTime' column to datetime format
data = pd.read_csv(f'{filepath}{filename}')
df = pd.DataFrame(data)

# Convert 'UTCReadTime' and 'ReadTime' columns to datetime format
df['UTCReadTime'] = pd.to_datetime(df['UTCReadTime'])
df['UTCReadTime'] = df['UTCReadTime'].dt.strftime('%m/%d/%Y %I:%M:%S %p')

df['ReadTime'] = pd.to_datetime(df['ReadTime'], format='%m/%d/%Y %I:%M:%S %p')
df['ReadTime'] = df['ReadTime'].dt.strftime('%m/%d/%Y %I:%M:%S %p')


other_weather = ['PressureHPA']

for col in other_weather:
    df[col] = fill_empty_with_adjacent(df[col].tolist(), direction="above")

df.to_csv(f'{filepath}OtherWeatherData_{filename}', index=False)
