"""
Author: Faizul Md Nor
Email(1): faizul.mdnoor@novasourcepower.com
Email(2): faizul.mdnoor@gmail.com

Requirements:
- Python 3.x
- Required packages: pandas, pytz

Usage:
1. Ensure the necessary Python packages are installed.
2. Configure the CMMS and database connection settings in fleet_performance.CMMS and
    fleet_performance.helpers.data_acquisition modules.
3. Run the script.
4. Follow the prompts to input analyst name for wildcard search.

Description:
This script retrieves work order data from a CMMS (Computerized Maintenance Management System) database for analysis.
It allows users to input an analyst's name for wildcard search and performs various analysis tasks including retrieving
work orders assigned to the analyst, filtering work orders reported by the analyst, and generating summary reports.

Modules:
- datetime: Provides classes for manipulating dates and times.
- pathlib: Provides classes for working with filesystem paths.
- pandas: Provides data structures and data analysis tools.
- pytz: Provides timezone handling functionalities.
- fleet_performance.CMMS: Contains the CMMS class for interacting with the CMMS database.
- fleet_performance.helpers.data_acquisition: Contains helper functions for data acquisition.

Functions:
1. confimation():
    - Prompts the user to confirm whether to continue with another analyst.
    - If the response is 'yes', calls the main() function.
    - If the response is 'no', exits the program.
    - If the response is invalid, prints an error message and exits the program.

2. no_input():
    - Exits the program if no input is provided.

3. get_month_dates():
    - Retrieves the start and end dates of the current month.
    - Returns the start date, end date, and current month in the format 'AbbreviatedMonthYYYY'.

4. get_sites_for_analyst(name: str) -> DataFrame or None:
    - Retrieves sites assigned to an analyst based on the analyst's name using wildcard search.
    - Args:
        - name (str): Analyst's name for wildcard search.
    - Returns:
        - df_sites (DataFrame or None): DataFrame containing site information if successful, otherwise None.

5. verify_analyst(df: DataFrame) -> Tuple[str, str] or None:
    - Verifies the analyst by checking if there's only one unique analyst in the DataFrame.
    - Args:
        - df (DataFrame): DataFrame containing site information.
    - Returns:
        - result (Tuple[str, str] or None): Tuple containing analyst substring and username if successful, otherwise
        None.

6. filter_current_month_wo(df: DataFrame, start_date: datetime) -> DataFrame:
    - Filters work orders for the current month based on the start date.
    - Args:
        - df (DataFrame): DataFrame containing work order data.
        - start_date (datetime): Start date of the current month.
    - Returns:
        - current_month_WO (DataFrame): DataFrame containing filtered work orders for the current month.

7. createfolder(name: str) -> str:
    - Creates a folder for saving CSV files based on the analyst's username.
    - Args:
        - name (str): Analyst's username.
    - Returns:
        - fullpath (str): Full path of the created folder.

8. get_work_orders_by_sites(df_sites: DataFrame, start_date: datetime, end_date: datetime, cmms: CMMS) -> DataFrame:
    - Retrieves work orders for assigned sites within a specified date range.
    - Args:
        - df_sites (DataFrame): DataFrame containing site information.
        - start_date (datetime): Start date of the date range.
        - end_date (datetime): End date of the date range.
        - cmms (CMMS): Instance of the CMMS class for interacting with the CMMS database.
    - Returns:
        - all_WOs (DataFrame): DataFrame containing all work orders for assigned sites.

9. reportedby_analyst(df2: DataFrame, uname: str) -> DataFrame or None:
    - Filters work orders reported by an analyst based on the analyst's username.
    - Args:
        - df2 (DataFrame): DataFrame containing work order data.
        - uname (str): Analyst's username.
    - Returns:
        - filter_reportedby (DataFrame or None): DataFrame containing filtered work orders reported by the analyst if
        successful, otherwise None.

10. main():
    - Main function to execute the fleet performance analysis tasks.
    - Calls other functions to retrieve data, perform analysis, and generate reports.

"""

# Import necessary modules and classes
from datetime import datetime
from pathlib import Path
import pandas as pd
import pytz #python timezone
from fleet_performance import CMMS
from fleet_performance.helpers.data_acquisition import dat

# Create an instance of the CMMS class
cmms = CMMS(superuser=True, environment='production')


def confimation():
    respond = input('Do you want to continue with another analyst? (yes/no): ')
    respond = respond.lower()
    while True:
        if ((respond == 'y') | (respond == 'yes')):
            print("Let's go!")
            main()
        if ((respond == 'n') | (respond == 'no')):
            print('Okay. Bye!')
            break
        else:
            print('Invalid input!')
            exit(0)


def no_input():
    exit(0)


# Function to get the start and end dates of the current month
def get_month_dates():
    now = datetime.now()
    cur_year = now.year
    cur_month = now.month
    year_month = now.strftime('%b%Y')
    start_date = datetime(cur_year, cur_month, 1)
    end_date = datetime(cur_year if cur_month < 12 else cur_year + 1, (cur_month % 12) + 1, 1)
    return start_date, end_date, year_month


# Function to retrieve sites assigned to an analyst based on the analyst's name
def get_sites_for_analyst(name):
    sql_query = f"""
        SELECT SCADA_GUID, 
                assettitle, 
                assetid, 
                OnM_Project_Status, 
                OnM_Area_Manager+';' AS [Area Manager],  
                OnM_Regional_Manager+';' AS [Regional Manager], 
                OnM_Site_Manager+';' AS [Site Manager], 
                [Performance Engineer Email(s)]+';' as [Performance Engineer], 
                OnM_Maintenance_Supervisor+';' AS [Maintenance Supervisor], 
                CAT_Analyst_Email as [Analyst]
        FROM Business.vwSites
        WHERE CAT_Analyst_Email LIKE '%{name}%'
        AND OnM_Project_Status = 'In Operation'
    """
    try:
        df_sites = dat.get_dataframe(sql_query)
        if df_sites.empty:
            print(f'{name} is not recognized.')
            return None  # Return None to indicate an error
        else:
            df_sites[['username', 'domain']] = df_sites['Analyst'].str.split('@', expand=True)
            df_sites['Analyst'] = df_sites['username'].apply(lambda x: x.lower())
    except Exception as err:
        print(f'Error occurred: {err}')
        return None  # Return None to indicate an error
    return df_sites


# Function to verify the analyst by checking if there's only one unique analyst in the dataframe
def verify_analyst(df):
    assignee_list = df['Analyst'].unique().tolist()
    if len(assignee_list) > 1:
        print('select 1 from below:')
        for name in assignee_list:
            print(name)
        return None
    else:
        username = str(assignee_list[0])
        substring = username[1:6]
    return substring, username


# Function to filter work orders for the current month
def filter_current_month_wo(df, start_date):
    start_date = start_date.replace(tzinfo=pytz.utc)
    current_month_WO = df[pd.to_datetime(df['createdDate']) > start_date]
    return current_month_WO


# Function to create a folder for saving CSV files
def createfolder(name):
    foldername = name + '/'
    parent_folder = 'C:/Users/FaizulBinMdNor/Documents/fleet-performance1.5.16/Scripting/Quiz_04/'
    fullpath = parent_folder + foldername
    Path(fullpath).mkdir(parents=True, exist_ok=True)
    return fullpath


# Function to retrieve work orders for assigned sites
def get_work_orders_by_sites(df_sites, start_date, end_date, cmms):
    all_WOs = pd.DataFrame()
    for _, row in df_sites.iterrows():
        print(f'Getting all WO for site {row["assettitle"]} from {start_date} to {end_date}')
        try:
            WOs = cmms.get_all_work_orders_by_site_title(site_asset_title=row["assettitle"],
                                                         start_date_time=start_date,
                                                         end_date_time=end_date)
            WOs = WOs[WOs['status'] != 'Canceled']
            all_WOs = pd.concat([all_WOs, WOs])
        except Exception as err:
            print(err)
    all_WOs.reset_index(drop=True, inplace=True)
    return all_WOs


# Function to filter work orders reported by an analyst
def reportedby_analyst(df2, uname):
    try:
        filter_reportedby = df2[(df2['reportedBy'] != '') & (df2['reportedBy'].str.contains(uname, case=False))]
        return filter_reportedby
    except Exception as error:
        print(error)
        return None


# Main code starts here
def main():
    # Get the start and end dates of the current month
    global username, wildcard, save_location
    start_date, end_date, current_month_year = get_month_dates()

    # Prompt the user to enter their name for wildcard search
    while True:
        analyst_name = input('Enter your name for wildcard search: ')
        if analyst_name == '':
            no_input()

        df_sites = get_sites_for_analyst(analyst_name)

        if df_sites is None:
            print(f"Error: Please use another wildcard search.")
            continue  # Prompt user to give another name

        result = verify_analyst(df_sites)
        if result is None:
            continue
        else:
            wildcard, username = result

        print('Assigned sites:')
        print(df_sites)
        save_location = createfolder(username)
        break  # Exit the loop if the name is recognized

    # Get work orders for assigned sites
    all_sites_WOs = get_work_orders_by_sites(df_sites, start_date, end_date, cmms)
    currents_month_WOs = filter_current_month_wo(all_sites_WOs, start_date)

    # Filter work orders reported by the analyst
    reportedBy = reportedby_analyst(all_sites_WOs, wildcard)

    if reportedBy is None:
        print(f'No WorkOrder(s) created by {username}')
    else:
        print(f'Number of Work Order(s) reported by {username}: {len(reportedBy)}')
        reportedBy.reset_index(drop=True, inplace=True)
        reportedBy.to_csv(f'{save_location}List WOs reportedby {username} {current_month_year}.csv', index=False)

    # Group plantName and save to CSV
    wos_by_sites = currents_month_WOs.groupby('plantName').size()
    df_all_open_wos_by_sites = pd.DataFrame(wos_by_sites, columns=['Number Of WOs']).sort_values(['Number Of WOs'])
    df_all_open_wos_by_sites.to_csv(f'{save_location}{username}_quiz4_question_2a.csv')

    # Pivot table by status and save to CSV
    wos_status_by_site = pd.pivot_table(currents_month_WOs, values='workOrderId', index=['plantName'],
                                        columns=['status'], aggfunc='count', fill_value=0)
    wos_status_by_site.to_csv(f'{save_location}{username}_quiz4_question_2b.csv')

    confimation()


# Execute the main function if the script is run directly
if __name__ == "__main__":
    main()
