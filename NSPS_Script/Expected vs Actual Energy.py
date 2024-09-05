from fleet_performance import Site
from fleet_performance.helpers import data_acquisition as dat
from fleet_performance.core.utils import time as tt
from fleet_performance.helpers.data_acquisition.utilities import get_dataframe, get_time_series_dataframe
from fleet_performance.helpers import sql_queries as qt
import pandas as pd

site = Site('OSF1')
SiteAssetID = site.SiteAssetID
start, stop = '2023-07-01', '2023-08-01'
daily_meter_summary_query = qt.get_daily_summary_sql_query(SiteAssetID, start, stop)
daily_meter_summary = get_time_series_dataframe(daily_meter_summary_query, datetime_column='ReadTimeDay')

revenue_energy_meter_data = dat.get_energy_meter_data(site.SiteAssetID, start, stop,
                                                  type_of_meter='Revenue',
                                                  columns=['KWTotal', 'KWhReceived', 'KWhDelivered'],
                                                  sum_meters_data=True)

# get TOD factors
minute_TOD_factors = dat.get_minute_time_of_day_factors(site.SiteAssetID, start, stop)

# FORMAT DATA

# separate data into power data and energy data dataframes
power = pd.DataFrame(revenue_energy_meter_data[['KWTotal']])
energy = pd.DataFrame(revenue_energy_meter_data[['KWhReceived', 'KWhDelivered']])

# DATA CLEANING
# join timetable to ensure all timestamps are present
ReadTime = tt.get_datetime_df([start], [stop], site, freq='min').set_index('ReadTime')
power = ReadTime.join(power, how='left')
energy = ReadTime.join(energy, how='left')

# ELIMINATE NANs by Forward Fill: Propagating Last Valid Observation Forward to Next Valid Value
energy = energy.fillna(method='ffill')

# set power generation during periods of power consumption to zero
power.loc[power['KWTotal'] > 0, 'KWTotal'] = 0

# get KWhReceived at first and last minute of the day
start_energy = energy.loc[energy.index.strftime('%H:%M') == '00:00']
stop_energy = energy.loc[energy.index.strftime('%H:%M') == '23:59']

# convert indices to only dates, so arithmetic can be performed
start_energy = start_energy.set_index(start_energy.index.date)
stop_energy = stop_energy.set_index(stop_energy.index.date)

# compute TOD power
power['TODKWTotal'] = power['KWTotal'] * minute_TOD_factors['FactorValue']

# compute daily energy
daily_energy_from_energy_difference = stop_energy - start_energy

# this value is called DailyKWh in the DailySummary table

# FORMAT CALCULATED DATA

# down-sample minute-level power data to day-level
daily_energy_from_power = -power.resample('D').sum() / 60
# this value is called TotalKWh in the DailySummary table
# this value is called TODDailyEnergyKWh in the DailySummary table

# get rid of final row containing first minute of next month
daily_energy_from_power = daily_energy_from_power[:-1]
daily_energy_from_energy_difference = daily_energy_from_energy_difference[:-1]

# make DailySummary dataframe with energy data
energy_DailySummary = pd.DataFrame()
energy_DailySummary['TotalKWh'] = daily_energy_from_power['KWTotal']
energy_DailySummary['DailykWh'] = daily_energy_from_energy_difference['KWhReceived']
energy_DailySummary['TODDailyEnergykWh'] = daily_energy_from_power['TODKWTotal']
energy_DailySummary['kWhDelivered'] = daily_energy_from_energy_difference['KWhDelivered']

energy_DailySummary.to_csv(f'{site.title}_energy_DailySummary_{start}-{stop}.csv')

df = dat.get_prediction_data(site.SiteAssetID, start, stop,
                             source_of_prediction='8760', type_of_prediction='contractual')

df2 = site.set_contractual_prediction_data(start, stop)

df3 = site.set_PlantPredict_prediction_data(start, stop)

df4 = dat.get_prediction_data(site.SiteAssetID, start, stop, source_of_prediction='PlantPredict', type_of_prediction='contractual')

df5 = site.set_power_from_scaled_expected(start,stop)

df6 = site.set_energy_from_scaled_expected(start, stop)

get_power_from_PlantPredict = site.set_power_from_PlantPredict_minute(start, stop, model_type='ideal')

df_scaled_expected_energy = site.set_power_from_scaled_expected('2023-01-01', '2023-12-31')