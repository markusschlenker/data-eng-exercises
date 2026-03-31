"""
pip install openmeteo-requests
pip install requests-cache retry-requests numpy pandas


"""

import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# Make sure all required weather variables are listed here
# The order of variables in hourly or daily is important to assign them correctly below
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 49.7939,
	"longitude": 9.9512,
	"hourly": ["temperature_2m", "showers", "rain", "weather_code", "apparent_temperature"],
	"past_days": 3,
	"forecast_days": 3,
}
responses = openmeteo.weather_api(url, params = params)

# Process first location. Add a for-loop for multiple locations or weather models
response = responses[0]
print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} m asl")
print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

# Process hourly data. The order of variables needs to be the same as requested.
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
hourly_showers = hourly.Variables(1).ValuesAsNumpy()
hourly_rain = hourly.Variables(2).ValuesAsNumpy()
hourly_weather_code = hourly.Variables(3).ValuesAsNumpy()
hourly_apparent_temperature = hourly.Variables(4).ValuesAsNumpy()

hourly_data = {"date": pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
)}

hourly_data["temperature_2m"] = hourly_temperature_2m
hourly_data["showers"] = hourly_showers
hourly_data["rain"] = hourly_rain
hourly_data["weather_code"] = hourly_weather_code
hourly_data["apparent_temperature"] = hourly_apparent_temperature

hourly_dataframe = pd.DataFrame(data = hourly_data)
print("\nHourly data\n", hourly_dataframe)

hourly_dataframe.to_csv("hourly_weather_data.csv", index = False)
print("\nSaved hourly weather data to hourly_weather_data.csv")

# WMO weather code documentation: https://open-meteo.com/en/docs?forecast_days=3&hourly=temperature_2m,showers,rain,weather_code,apparent_temperature&past_days=3&latitude=49.7939&longitude=9.9512#weather_variable_documentation
#print(set(hourly_data["weather_code"]))

# Line chart for temperature variables using DataFrame.plot
import matplotlib.pyplot as plt

ax = hourly_dataframe.plot(
    x="date",
    y=["temperature_2m", "apparent_temperature"],
    figsize=(12, 6),
    linewidth=1.5,
    title="Hourly 2m Temperature and Apparent Temperature",
)
ax.set_xlabel("Datetime (UTC)")
ax.set_ylabel("Temperature (°C)")
ax.grid(True)

plt.tight_layout()
output_image = "hourly_temperature_chart.png"
plt.savefig(output_image, dpi=150)
print(f"Saved line chart to {output_image}")
plt.show()

