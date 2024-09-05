from fleet_performance import Site
from datetime import datetime, timedelta
site =Site('BFR1')

start_date, stop_date = '2023-11-01', '2023-12-01'
start_time, stop_time = '07:30', '09:30'

start_date = datetime.strptime(start_date, '%Y-%m-%d')
stop_date = datetime.strptime(stop_date, '%Y-%m-%d')

def null_timeframe(from_dt, to_dt):
    start_null, stop_null = from_dt, to_dt

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

def create_time_frame(start_date, stop_date, start_time, stop_time):
    current_date = start_date
    while current_date <= stop_date:
        process_date = current_date.strftime('%Y-%m-%d')
        from_date_time = f'{process_date} {start_time}'
        from_date_time = datetime.strptime(from_date_time, '%Y-%m-%d %H:%M')
        to_date_time = f'{process_date} {stop_time}'
        to_date_time = datetime.strptime(to_date_time, '%Y-%m-%d %H:%M')
        # print(f'from {from_date_time} to {to_date_time}')
        null_timeframe(from_date_time, to_date_time)
        current_date += timedelta(days=1)

create_time_frame(start_date, stop_date, start_time, stop_time)
