from fleet_performance import Site
from datetime import datetime

site = Site('BFR1')

null_date = ['2023-11-02', '2023-11-03', '2023-11-05', '2023-11-08', '2023-11-10', '2023-11-08', '2023-11-10',
             '2023-11-11', '2023-11-15', '2023-11-16', '2023-11-18', '2023-11-19', '2023-11-22', '2023-11-29']

start_time, stop_time = '07:30', '09:30'


def null_timeframe(start_null, stop_null):

    weather_station_titles = ['BFR1_B001_P006.WeatherStation']
    target_columns = ['Irradiance1']
    # target_columns = ['Irradiance2']
    # target_columns = ['RelativeHumidity', 'PressureHPA']
    # target_columns = ['RelativeHumidity']
    # target_columns = ['PressureHPA']
    # target_columns = ['RelativeHumidity', 'TemperatureC', 'PressureHPA', 'WindSpeedMpS']
    # target_columns = ['PressureHPA']
    site.run_weather_station_data_null(start_null, stop_null, weather_station_titles, target_columns)
    print(f'Nulling {target_columns} for {weather_station_titles} from {start_null} to {stop_null}')


for date in null_date:
    start_date_time = f'{date} {start_time}'
    stop_date_time = f'{date} {stop_time}'
    start_date = datetime.strptime(start_date_time, '%Y-%m-%d %H:%M')
    stop_date = datetime.strptime(stop_date_time, '%Y-%m-%d %H:%M')
    print("Start Date:", start_date)
    print("Stop Date:", stop_date)
    null_timeframe(start_date, stop_date)
