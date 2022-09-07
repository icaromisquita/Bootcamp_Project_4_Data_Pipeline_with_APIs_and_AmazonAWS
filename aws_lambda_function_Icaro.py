from datetime import datetime, timedelta, date, time
import requests
import pymysql
import pandas as pd
import numpy as np
#from IPython.display import JSON
import unicodedata
import json
import bs4 as bs


schema="Name of your Schema"
host="Your Amazon RDS adress"
user="root"
password="Your Password"
port=3306
con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'

####API Keys

OWM_key = "Your API KEY"
flight_api_key = "Your API KEY"


###### Obtaining the Data
api_input = pd.read_csv("https://raw.githubusercontent.com/icaromisquita/archives/main/Api_input.csv")

icao = api_input["airport_icao"].tolist()



##Flight API
    # Berlin
berlin_icao = icao[0]
to_local_time = datetime.now().strftime('%Y-%m-%dT%H:00')
from_local_time = (datetime.now() + timedelta(hours=9)).strftime('%Y-%m-%dT%H:00')
url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{berlin_icao}/{to_local_time}/{from_local_time}"

querystring = {"withLeg":"true","withCancelled":"true","withCodeshared":"true","withCargo":"true","withPrivate":"false","withLocation":"false"}

headers = {
    'x-rapidapi-host': "aerodatabox.p.rapidapi.com",
    'x-rapidapi-key': flight_api_key
    }

berlin_flights= requests.request("GET", url, headers=headers, params=querystring)

arrivals_berlin = berlin_flights.json()['arrivals']
berlin_icao = icao[0]

def get_flight_info(flight_json):
    # terminal
    try: terminal = flight_json['arrival']['terminal']
    except: terminal = None
    # aircraft
    try: aircraft = flight_json['aircraft']['model']
    except: aircraft = None

    return {
        'dep_airport':flight_json['departure']['airport']['name'],
        'sched_arr_loc_time':flight_json['arrival']['scheduledTimeLocal'],
        'terminal':terminal,
        'status':flight_json['status'],
        'aircraft':aircraft,
        'icao_code': berlin_icao 
    }

arrivals_berlin = pd.DataFrame([get_flight_info(flight) for flight in arrivals_berlin])

    # London
london_icao = icao[1]
url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{london_icao}/{to_local_time}/{from_local_time}"

querystring = {"withLeg":"true","withCancelled":"true","withCodeshared":"true","withCargo":"true","withPrivate":"false","withLocation":"false"}

london_flights = requests.request("GET", url, headers=headers, params=querystring)

arrivals_london = london_flights.json()['arrivals']
london_icao = icao[1]

def get_flight_info(flight_json):
    # terminal
    try: terminal = flight_json['arrival']['terminal']
    except: terminal = None
    # aircraft
    try: aircraft = flight_json['aircraft']['model']
    except: aircraft = None

    return {
        'dep_airport':flight_json['departure']['airport']['name'],
        'sched_arr_loc_time':flight_json['arrival']['scheduledTimeLocal'],
        'terminal':terminal,
        'status':flight_json['status'],
        'aircraft':aircraft,
        'icao_code': london_icao 
    }

arrivals_london = pd.DataFrame([get_flight_info(flight) for flight in arrivals_london])

#### Weather API

city_name = api_input["Name"].tolist()
country_name = api_input["ISO_3166_code"].tolist()

    #For Berlin
city_name = api_input["Name"].tolist()
country_name = api_input["ISO_3166_code"].tolist()

# achieve the same result with the weather api
response_berlin = requests.get(f'http://api.openweathermap.org/data/2.5/forecast/?q={city_name[0]},{country_name[0]}&appid={OWM_key}&units=metric&lang=en')

response_berlin.json()
forecast_api = response_berlin.json()['list']
# look for the fields that could ve relevant: 
# better field descriptions https://www.weatherbit.io/api/weather-forecast-5-day

weather_info = []

for forecast_3h in forecast_api: 
    weather_hour = {}
    # datetime utc
    weather_hour['datetime'] = forecast_3h['dt_txt']
    # temperature 
    weather_hour['temperature'] = forecast_3h['main']['temp']
    # wind
    weather_hour['wind'] = forecast_3h['wind']['speed']
    # probability precipitation 
    try: weather_hour['prob_perc'] = float(forecast_3h['pop'])
    except: weather_hour['prob_perc'] = 0
    # rain
    try: weather_hour['rain_qty'] = float(forecast_3h['rain']['3h'])
    except: weather_hour['rain_qty'] = 0
    # wind 
    try: weather_hour['snow'] = float(forecast_3h['snow']['3h'])
    except: weather_hour['snow'] = 0
    weather_hour['municipality_iso_country'] = city_name[0] + ',' + country_name[0] #Check it
    weather_info.append(weather_hour)    
    
weather_data_berlin = pd.DataFrame(weather_info)

    #For London
response_london = requests.get(f'http://api.openweathermap.org/data/2.5/forecast/?q={city_name[1]},{country_name[1]}&appid={OWM_key}&units=metric&lang=en')
response_london.json()

forecast_api = response_london.json()['list']
# look for the fields that could ve relevant: 
# better field descriptions https://www.weatherbit.io/api/weather-forecast-5-day

weather_info = []

# datetime, temperature, wind, prob_perc, rain_qty, snow = [], [], [], [], [], []
for forecast_3h in forecast_api: 
    weather_hour = {}
    # datetime utc
    weather_hour['datetime'] = forecast_3h['dt_txt']
    # temperature 
    weather_hour['temperature'] = forecast_3h['main']['temp']
    # wind
    weather_hour['wind'] = forecast_3h['wind']['speed']
    # probability precipitation 
    try: weather_hour['prob_perc'] = float(forecast_3h['pop'])
    except: weather_hour['prob_perc'] = 0
    # rain
    try: weather_hour['rain_qty'] = float(forecast_3h['rain']['3h'])
    except: weather_hour['rain_qty'] = 0
    # wind 
    try: weather_hour['snow'] = float(forecast_3h['snow']['3h'])
    except: weather_hour['snow'] = 0
    weather_hour['municipality_iso_country'] = city_name[1] + ',' + country_name[1] 
    weather_info.append(weather_hour)    
    
weather_data_london = pd.DataFrame(weather_info)

###Population Data

cities = ['Berlin','Paris','Amsterdam','Barcelona','Rome','Lisbon','Prague','Vienna','Madrid']

#Obtaining cities info from Wikipedia
list_of_city_info = []
for city in cities:
    url = 'https://en.wikipedia.org/wiki/{}'.format(city)
    web = requests.get(url,'html.parser')
    soup = bs(web.content, 'lxml')
    list_of_city_info.append(City_info(soup))
df_cities = pd.DataFrame(list_of_city_info)

###Airports Data
airports_cities = (
pd.read_csv('https://raw.githubusercontent.com/icaromisquita/archives/main/airports.csv')
    .query('type == "large_airport"')
    .filter(['name','latitude_deg','longitude_deg','iso_country','iso_region','municipality','gps_code','iata_code'])
    .rename(columns={'gps_code':'icao_code'})
    .assign(municipality_iso_country = lambda x: x['municipality'] + ',' + x['iso_country'])
)
    #Concatenating arrivals data for all airports
arrivals_data = pd.concat([arrivals_berlin, arrivals_london], axis=0 )
arrivals_data.reset_index(drop = True, inplace=True)
weather_data = pd.concat([weather_data_berlin, weather_data_london], axis=0 )
weather_data.reset_index(drop = True, inplace=True)
cities = airports_cities.filter(['municipality','iso_country','municipality_iso_country']).drop_duplicates()
arrivals_data = airports_cities.merge(arrivals_data, on='icao_code', how='inner').merge(weather_data, on='municipality_iso_country', how='inner').head()

df_cities['municipality_iso_country'] = [
    'Berlin,DE',
    'Paris,FR',
    'Amsterdam,NL',
    'Barcelona,ES',
    'Rome,IT',
    'Lisbon,PT',
    'Prague,CZE',
    'London,GB',
    'Madrid,ES'
]   
        
#### Updating the files to the Database  
    #Arrivals
(
arrivals_berlin
    .replace({np.nan},'unknown')
    .assign(sched_arr_loc_time = lambda x: pd.to_datetime(x['sched_arr_loc_time']))
    .to_sql('arrivals', if_exists='append', con=con, index=False))

    #Airports
    
airports_cities.dropna().to_sql('airports', if_exists='append', con=con, index=False)

    #Cities
(
df_cities
    .dropna()
    .rename(
        columns={
            'lat':'latitude',
            'long':'longitude'
            }
        )
    .to_sql('cities', con=con, if_exists='append', index=False)
    )

    #Weather
    
weather_data.assign(datetime = lambda x: pd.to_datetime(x['datetime'])).to_sql('weather', if_exists='append', con=con, index=False)
