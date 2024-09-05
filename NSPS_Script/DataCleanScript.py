##%% Viewing Data
from fleet_performance import Site
from fleet_performance import data_acquisition as dat

site = Site('TKY1')
start, stop = '2024-03-01', '2024-04-01'

site.plot_irradiance(start, stop, view=True)
site.plot_other_weather_data(start, stop, view=True)
site.run_weather_station_data_evaluation(start, stop)
site.plot_irradiance(start, stop, layer='Business', show_expected_POA=True, include_clear_sky=False)
site.plot_revenue_energy_meter_data(start, stop, layer="Business")

weather_meta = site.weather_station_metadata
wm = dat.get_weather_station_metadata(site.SiteAssetID)
print(wm['AssetTitle'])
print(site.SCADA_GUID)
print(site.SiteAssetID)

nearby_site = site.set_peers(150)  # by default () is 50 KM

# site.run_energy_meter_data_clean(start, stop, target_columns = ['KWTotal' , 'KWhReceived'])

##
# Plot multiple sites weather station.
start_date, stop_date = '2023-08-01', '2023-08-02'
site.plot_and_set_multiple_site_met_data(start_date, stop_date, layer='Business', mean=False, peers=True, radius=100)

##%% Nulling Data

start_null, stop_null = '2024-03-01', '2024-04-01'  # Dates can be given as yyyy-mm-dd hh:mm

weather_station_titles = ['TKY1_B01B_P015.WeatherStation', 'TKY1_B01B_P013.WeatherStation']
target_columns = ['Irradiance1']  # POA
# target_columns = ['Irradiance2']  # GHI
# target_columns = ['RelativeHumidity', 'TemperatureC']
# target_columns = ['RelativeHumidity']
# target_columns = ['PressureHPA']
# target_columns = ['RelativeHumidity', 'TemperatureC', 'PressureHPA', 'WindSpeedMpS']
# target_columns = ['PressureHPA']
site.run_weather_station_data_null(start_null, stop_null, weather_station_titles, target_columns)
# site.run_weather_station_data_clean(start, stop, target_assets=[],
#                                     target_columns=["WindSpeedMpS", "TemperatureC", "PressureHPA", "RelativeHumidity"],
#                                     upload_files='False')

##%% Infilling Data

# target_columns = ['WindSpeedMpS', 'TemperatureC', 'RelativeHumidity','PressureHPA']

start_infill, stop_infill = '2024-03-01', '2024-04-01'

# ['Irradiance2'] = GHI
# ['Irradiance1'] = POA

# target_columns = ['WindSpeedMpS', 'TemperatureC', 'PressureHPA', 'RelativeHumidity']
target_columns = ['Irradiance1']
# target_columns = ['PressureHPA']
# target_columns = ['WindSpeedMpS']
# target_columns = ['TemperatureC']

site.mask_weather_station_data(start_infill, stop_infill, site_from='BFR1',
                               infill_only_nulls=True,
                               infill_calc_poa_from_ghi=True,
                               cols_replace=target_columns,
                               interpolate_min=30,
                               upload_to_pop=True)

##%% Nearby sites
from fleet_performance import Site

site = Site('MLG1')
site.set_peers(100)
site_keys = site.SCADA_GUID

##%%Looping Data
from fleet_performance import Site

site = Site('BGM1')
assetList = ['BFR1_B003_P011.WeatherStation']
m = '03'
d1t1, d1t2 = '07:00', '10:00'
# d2t1, d2t2 = '17:40', '20:00'

for d in range(1, 15):
    print(str(d))
    start, stop = f'2024-{m}-{d} {d1t1}', f'2024-{m}-{d} {d1t2}'
    site.run_weather_station_data_null(start, stop, weather_station_titles=assetList, target_columns=["Irradiance1"])
    print(start, ' ', stop)

##%% Nulling Data by limit

start_clean, stop_clean = '2024-01-01', '2024-02-01'

# target_columns = ["Irradiance1", "Irradiance2", "WindSpeedMpS", "TemperatureC", "PressureHPA", "RelativeHumidity"]
# target_columns = ["WindSpeedMpS", "TemperatureC", "RelativeHumidity", "PressureHPA"]
# target_columns = ['RelativeHumidity', 'PressureHPA']
target_columns = ['WindSpeedMpS']
site.run_weather_station_data_clean(start_clean, stop_clean,
                                    target_assets=[906103, 906104, 906105],
                                    target_columns=target_columns,
                                    upload_files=True)

##%% EM Data

from fleet_performance import data_acquisition as dat

site = Site('BGM1')
site.plot_revenue_energy_meter_data(start, stop, layer="Business")

start_clean, stop_clean = '2024-02-19', '2024-02-19'
site.run_energy_meter_data_clean(start_clean, stop_clean, target_columns=['KWTotal', 'KWhReceived'])

##%% no interpolation
dfem = dat.get_energy_meter_data(site.SiteAssetID, start, stop,
                                 type_of_meter='Revenue',
                                 resolution=1,
                                 columns=['*'],
                                 sum_meters_data=False,
                                 layer='Business')

dfem.reset_index(inplace=True)
dfem.plot(x='ReadTime', y=['KWhReceived', 'KWhDelivered', "KWTotal"], subplots=True)

##%% EM data infill

site = Site('BGM1')

startEM, stopEM = '2024-02-09', '2024-02-13'

site.run_energy_meter_data_infill(startEM, stopEM, ignore_KWhDelivered=True)

##%% Nulling Data by limit

from fleet_performance import Site

site = Site('DES2')
start, stop = '2024-01-01', '2024-02-01'

# target_columns = ["WindSpeedMpS", "TemperatureC", "PressureHPA", "RelativeHumidity"]
target_columns = ["RelativeHumidity"]
asset_ws = ['DES2_B001_P004.WeatherStation', 'DES2_B001_P008.WeatherStation', 'DES2_B002_P013.WeatherStation',
            'DES2_B002_P014.WeatherStation', 'DES2_B002_P015.WeatherStation']
site.run_weather_station_data_clean(start, stop,
                                    target_assets=asset_ws,
                                    target_columns=target_columns,
                                    upload_files=True)

## Plant Energy / Predicted energy based upon Measured Weather data
from fleet_performance import Site

site = Site('BFR1')
start, stop = '2022-12-01', '2022-12-27'

site.plot_performance_index(start, stop, 'D', 'scaled_expected')
site.run_outage_evaluation(start, stop)

## Compare MET station. POA and others weather station.
start_met, stop_met = '2023-08-16', '2023-08-17'
site.plot_and_set_multiple_site_met_data(start_met, stop_met, mean=False, peers=True, radius=50, layer='Business',
                                         view=True)
# EM
start_em_infill, stop_em_infill = '2023-12-31', '2024-02-02'
site.run_energy_meter_data_infill(start_em_infill, stop_em_infill, ignore_KWhDelivered=True)
site.run_energy_meter_data_clean(start_em_infill, stop_em_infill)
