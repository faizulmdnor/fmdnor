import pandas as pd
import os
from datetime import datetime

folder_path = 'C:/Users/fmdno/Dropbox/My Resume/Job Search/'
file_name = 'Job_search_log.csv'
today = datetime.today().strftime("%d-%m-%Y")
job = pd.read_csv(folder_path+file_name)

def get_new_job_log():
    # Input new job
    company = input('Company name: ')
    position = input('Position: ')
    location = input('Location: ')
    date_applied = input('Enter date applied ("DD-MM-YYYY"): ')
    status = input('Enter status: ')
    date_status = datetime.now().strftime("%d-%m-%Y %H:%M")
    application_method = input("Enter application method ('online', 'walk-in', 'others'): ")

    # Handle application method-specific information
    if application_method == 'online':
        information = input('Enter website URL: ')
    elif application_method == 'walk-in':
        information = input('Enter contact person or address: ')
    else:
        information = input('Enter related information about the application: ')

    # Create a temporary DataFrame with the new data
    df = pd.DataFrame({
        'Date': [today],
        'Applied Date': [date_applied],
        'Company': [company],
        'Position Applied': [position],
        'Location': [location],
        'Status': [status],
        'Status Date': [date_status],
        'Application': [application_method],
        'Information': [information]
    })
    return df


# Initialize an empty DataFrame
df_job = pd.DataFrame(job)

# Loop to allow multiple job log entries
a = input("Enter new job log (yes/no): ").lower()
while a.lower() == 'yes':
    # Get new job log entry and append to df_job
    temp_df = get_new_job_log()
    df_job = pd.concat([df_job, temp_df], ignore_index=True)

    # Ask if user wants to enter another job log
    a = input("Enter another job log (yes/no): ").lower()

# Save the DataFrame to a CSV file
df_job.to_csv(os.path.join(folder_path, file_name), index=False)
