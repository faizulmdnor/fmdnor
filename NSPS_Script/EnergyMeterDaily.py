# Import necessary libraries
import pandas as pd
from datetime import datetime

# Get the current date and time
today = datetime.now()
t = today.strftime('%Y-%m-%d')

# Define file paths and names
filepath = 'C:/Users/FaizulBinMdNor/Downloads/'
filename = 'Template13760.csv'

# Read the CSV file into a pandas DataFrame
data = pd.read_csv(filepath + filename)

# Convert 'ReadTime' column to datetime
data['ReadTime'] = pd.to_datetime(data['ReadTime'])

# Create a new 'date' column with only the date part
data['date'] = data['ReadTime'].dt.strftime('%m/%d/%Y')

# Identify and store duplicate 'UTCReadTime' values
dup_utcreadtime = data[data['UTCReadTime'].duplicated()]
print(f'Number of duplicate UTCReadTime: {dup_utcreadtime}')

# Identify and store duplicate 'ReadTime' values
dup_readtime = data[data['ReadTime'].duplicated()]
print(f'Number of duplicate ReadTime: {dup_readtime}')

# Check for duplicate 'ReadTime' values and keep the last occurrence
data.drop_duplicates('ReadTime', keep='last', inplace=True)

# Create a new column 'New KW Total' with KWTotal multiplied by -1
data['New KW Total'] = data['KWTotal'] * -1

# Save the modified DataFrame to a new CSV file with a timestamp
data.to_csv(f'{filepath}{filename}_{t}.csv', index=False)

# Group the data by date and calculate the sum of 'KWTotal' for each date
KWTotal_date = data.groupby('date')['KWTotal'].sum().reset_index()

# Create a new 'KWhtotal' column by dividing 'KWTotal' by -60
KWTotal_date['KWhtotal'] = KWTotal_date['KWTotal'] / -60

# Save the grouped DataFrame to a new CSV file
KWTotal_date.to_csv(filepath + filename + '_edit.csv', index=False)

# Print the resulting DataFrame
print(KWTotal_date)
