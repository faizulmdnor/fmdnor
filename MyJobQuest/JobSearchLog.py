from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt

# File paths and read data
folder_path = 'C:/Users/fmdno/Dropbox/My Resume/Job Search/'
file_name = 'Job_search_log.csv'
today = datetime.today().strftime("%d-%m-%Y")
job = pd.read_csv(folder_path + file_name)
jobs = pd.DataFrame(job)
jobs['No.'] = job.index + 1

# Convert 'Applied Date' to a standard format
new_applied_date = []
date_strings = job['Applied Date'].tolist()
for date_string in date_strings:
    try:
        date_obj = datetime.strptime(date_string, '%d-%m-%Y')
    except ValueError:
        date_obj = datetime.strptime(date_string, '%d/%m/%Y')
    new_applied_date.append(date_obj.strftime('%d-%m-%Y'))

jobs['Applied Date'] = new_applied_date
jobs['Month'] = pd.to_datetime(jobs['Applied Date'], format='%d-%m-%Y').dt.month_name()

# Count jobs by month
monthly_jobs = jobs['Month'].value_counts().sort_index()

# Count jobs by status
status_counts = jobs['Status'].value_counts()

# Count jobs by days
daily_jobs = jobs.groupby('Applied Date').size().reset_index(name='Job Count')
daily_jobs['Applied Date'] = pd.to_datetime(daily_jobs['Applied Date'], format='%d-%m-%Y')
daily_jobs = daily_jobs.sort_values(by='Applied Date', ascending=True)
daily_jobs.reset_index(drop=True, inplace=True)

# Calculate number of days from start_date to today
start_date = daily_jobs['Applied Date'].iloc[0]
num_of_days = (datetime.today() - start_date).days

# Create a DataFrame with a date range and initialize counts
date_list = [start_date + timedelta(days=x) for x in range(num_of_days + 1)]
df2_date = pd.DataFrame(date_list, columns=['Date'])
df2_date.set_index('Date', inplace=True)

# Merge with daily_jobs to fill in counts
df2_date = df2_date.join(daily_jobs.set_index('Applied Date'), how='left').reset_index()

# Fill NaN values with 0 for days with no jobs
df2_date['Job Count'].fillna(0, inplace=True)

# Display the updated DataFrame
print(df2_date)

# Create plots
fig, axs = plt.subplots(3, 1, figsize=(12, 18))

# Number of jobs applied by month plot
monthly_jobs.plot(kind='bar', ax=axs[0], color='skyblue')
axs[0].set_title('Number of Jobs Applied by Month')
axs[0].set_xlabel('Month')
axs[0].set_ylabel('Number of Jobs')
axs[0].tick_params(axis='x', rotation=45)

# Add table values to the monthly jobs bar chart
for i, value in enumerate(monthly_jobs):
    axs[0].text(i, value + 0.5, str(value), ha='center', va='bottom')

# Number of jobs by day plot
df2_date.plot(kind='line', x='Date', y='Job Count', ax=axs[1], color='lightcoral')
axs[1].set_title('Number of Jobs by Day')
axs[1].set_xlabel('Date')
axs[1].set_ylabel('Job Count')

# Add table values to the daily jobs line chart
for i, value in df2_date.iterrows():
    axs[1].text(value['Date'], value['Job Count'] + 0.5, str(int(value['Job Count'])), ha='center', va='bottom')

# Number of jobs by status plot
status_counts.plot(kind='bar', ax=axs[2], color='lightcoral')
axs[2].set_title('Number of Jobs by Status')
axs[2].set_xlabel('Job Status Category')
axs[2].set_ylabel('Number of Jobs')
axs[2].tick_params(axis='x', rotation=0)

# Add table values to the status bar chart
for i, value in enumerate(status_counts):
    axs[2].text(i, value + 0.5, str(value), ha='center', va='bottom')

# Adjust layout
plt.tight_layout()

# Show the plots
plt.show()
