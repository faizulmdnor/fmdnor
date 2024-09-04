import requests
import pandas as pd

# Define API key and list of locations
api_key = "bdd400b81266413abf420338240409"
locations = ['Malaysia', 'Thailand', 'Singapore', 'Indonesia', 'Bali', 'Phuket', 'Kulim', 'Alor Setar', 'Penang']

def get_weather_data(api_key, location):
    """
    Fetches weather data from the Weather API for a given location.

    Args:
    api_key (str): API key for authentication.
    location (str): Name of the location for weather data.

    Returns:
    dict: JSON response containing weather data, or None if an error occurs.
    """
    url = f"https://api.weatherapi.com/v1/current.json?key={api_key}&q={location}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {location}: {e}")
        return None

def process_data(data):
    """
    Processes the weather data to combine location and current weather information.

    Args:
    data (dict): Weather data containing 'location' and 'current' keys.

    Returns:
    pd.DataFrame: A DataFrame with combined location and weather data.
    """
    if data:
        location_data = pd.json_normalize(data['location'])
        current_data = pd.json_normalize(data['current'])
        combined_data = pd.concat([location_data, current_data], axis=1)
        return combined_data
    else:
        return pd.DataFrame()

# Initialize an empty DataFrame to store all weather data
df_weather_data = pd.DataFrame()

# Loop through each location, fetch and process data, then concatenate into a single DataFrame
for loc in locations:
    weather_data = get_weather_data(api_key, loc)
    if weather_data:
        df = process_data(weather_data)
        df_weather_data = pd.concat([df_weather_data, df], ignore_index=True)
    else:
        print(f"No data available for {loc}")

# Display the final DataFrame
print(df_weather_data)
