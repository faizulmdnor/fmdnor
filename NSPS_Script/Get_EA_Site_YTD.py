from datetime import datetime, timedelta
from fleet_performance import data_acquisition as dat
import pandas as pd


def generate_sql_script(scada_guid, start_year, end_year):
    sql_script = f"DECLARE @SITE VARCHAR(10) = (SELECT AssetID FROM Business.vwSites WHERE SCADA_GUID = '{scada_guid}')\n\n"

    for year in range(start_year, end_year):
        for month in range(1, 13):
            # Calculate the start and end dates for each month
            start_date = datetime(year, month, 1).strftime('%Y-%m-%d')
            end_date = (datetime(year, month, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            end_date = end_date.strftime('%Y-%m-%d')

            sql_script += "SELECT '{}' AS [month], *\n".format(str(month).zfill(2))
            sql_script += "FROM [GlobalFED].[Business].[udfTBGetEffectiveAvailabilityByDateRange](@SITE, '{}', '{}', 0) AS FUN \n".format(start_date, end_date)
            sql_script += "UNION\n"

    # Remove the trailing "UNION" from the last SELECT statement
    sql_script = sql_script.rstrip("UNION\n")

    return sql_script

# Example usage

start_year_variable = 2023
end_year_variable = 2024

file = 'C:/Users/FaizulBinMdNor/OneDrive - NovaSource Power Services/Documents/Monthly Reporting/EA/Sites_List.csv'
data = pd.read_csv(file)
df_data = data[['SCADA GUID', 'Status']]
df_data = df_data[df_data['Status'] == 'In Operation']
scada_guid_list = df_data['SCADA GUID'].to_list()

for i in range(len(scada_guid_list)):
    scada_guid_variable = scada_guid_list[i]
    sql_script = generate_sql_script(scada_guid_variable, start_year_variable, end_year_variable)
    ea = dat.get_dataframe(sql_script)
    ea.to_csv(f'C:/Users/FaizulBinMdNor/OneDrive - NovaSource Power Services/Documents/Monthly Reporting/EA/{scada_guid_list[i]}_EA.csv', index=False)
