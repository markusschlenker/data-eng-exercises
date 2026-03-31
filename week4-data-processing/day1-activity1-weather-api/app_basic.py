"""

"""

import requests as req
import pandas as pd
import json

url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 49.7939,
	"longitude": 9.9512,
	"hourly": ["temperature_2m", "showers", "rain", "weather_code", "apparent_temperature"],
	"past_days": 3,
	"forecast_days": 3,
}
response = req.get(url, params = params)
#print(response.json().keys())
from pprint import pprint
#pprint(response.json())
#print(response.json()["hourly"].keys())

hourly_data = response.json()["hourly"]
df = pd.DataFrame(hourly_data)

wmo_codes_str = """\
0 	Clear sky
1 	Mainly clear
2 	Partly cloudy
3 	Overcast
45 	Fog
48 	Depositing rime fog
51 	Light drizzle
53 	Drizzle
55 	Dense drizzle
61 	Slight rain
63 	Rain
65 	Heavy rain
80 	Rain showers
95 	Thunderstorm
"""
df_wmo = pd.DataFrame(
	[(int(l[:2].strip()), l[3:].strip()) for l in wmo_codes_str.splitlines()],
	columns=["weather_code", "description"],
)
df["weather_description"] = pd.merge(df, df_wmo, on="weather_code")["description"]
print(df)

df.to_csv("hourly_weather_data__basic_approach.csv", index=False)
print("Saved hourly weather data to hourly_weather_data.csv")