from datetime import datetime, timedelta, date, time

import requests
import pymysql
import pandas as pd
  

def connect_sql():
    schema="Name of your database"
    host="127.0.0.1"
    user="root"
    password="Your Password"
    port=3306
    con = f'mysql+pymysql://{user}:{password}@{host}:{port}/{schema}'
    return con


# import cities df
def get_cities(con):
    cities = pd.read_sql('SELECT * FROM cities', con)
    return cities


# get information on cities chosen by user
def get_current_cities(city_country_dict, cities):
    city_data_list = []

    for city, country in city_country_dict.items():

        # get information on city and store it for output to city_data table
        current_city = cities.loc[(cities['country'] == country) & ((cities['city_ascii'] == city) | (cities['city'] == city))]
        city_data_list.append(current_city)
    
    city_data = pd.concat(city_data_list)

    return city_data


# call openweather API to get weather data    
def get_weather(city_country_dict, city_data, datetime_start, datetime_end):
    
    # define two empty lists that will store the data to be concatenated into two output dataframes at the end of the function
    forecast_data_list = []

    for city, country in city_country_dict.items():
        url = f'http://api.openweathermap.org/data/2.5/forecast?q={city}&appid=OPENWEATHER_API_KEY&units=metric'
        response = requests.get(url)
        weather = response.json()

        # get information on current city and store it in variable
        current_city = city_data.loc[(city_data['country'] == country) & ((city_data['city_ascii'] == city) | (city_data['city'] == city))]

        # get weather forecast information (numerical) and outlook on sky (strings)
        weather_forecast_words = []
        for i in range(len(weather['list'])):
            weather_forecast = pd.json_normalize(weather['list'][i])
            weather_words = pd.json_normalize(weather['list'][i]['weather'])
            weather_forecast_words.append(pd.concat([weather_forecast, weather_words], axis= 1))

        # concatenate city information (unique identifiers city_id and city_name) and forecast data to allow for easy merging of city and weather data
        combined_forecast_cityname = []
        cols = ['city_ascii', 'country', 'city_id']

        for forecast in weather_forecast_words:
            combined_forecast_cityname.append(pd.concat([current_city[cols].reset_index(), forecast], axis= 1))

        forecast_df = pd.concat(combined_forecast_cityname)
        forecast_data_list.append(forecast_df)

        # clean the resulting data by removing uninteresting columns and renaming all others to be more descriptive
        forecast_df = pd.concat(forecast_data_list).drop(columns= ['weather', 'main.temp_kf', 'icon', 'index'])
        forecast_df.columns = (['city_ascii', 'country','city_id', 'timestamp', 'avg_visibility_m', 
            'precipitation_probability', 'date_and_time', 'temp', 'felt_temp', 'min_temp', 'max_temp', 'pressure', 
            'pressure_sea_lvl', 'pressure_ground_lvl', 'humidity_perc', 'cloudiness_perc', 'wind_speed_m_per_sec', 
            'wind_direction_degrees', 'wind_gust_m_per_sec', 'night_or_day', 'outlook_id', 'outlook_short', 'outlook_long', 
            'rain_vol_last_3_hrs'])

    # convert date and time column to datetime
    forecast_df['date_and_time'] = pd.to_datetime(forecast_df['date_and_time'])
    forecast_df = forecast_df[forecast_df['date_and_time'].between(datetime_start, datetime_end)]

    # return two dataframes: 
    # 1. forecast containing weather information
    # 2. city containing city information
    return forecast_df   


# create airport dict with icao codes, names, and city
def get_airport_dict(city_country_dict, city_data):
    icao_list = []
    airport_name_list = []
    city_name_list = []
    city_id_list = []

    for city, country in city_country_dict.items():
        url = "https://aerodatabox.p.rapidapi.com/airports/search/term"

        querystring = {"q":f"{city}","limit":"10"}

        headers = {
            'X-RapidAPI-Key': 'X-RAPID-API-KEY',
            'X-RapidAPI-Host': 'aerodatabox.p.rapidapi.com'
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
        airports = response.json()

        current_city = city_data.loc[(city_data['country'] == country) & ((city_data['city_ascii'] == city) | (city_data['city'] == city))]

        for airport in airports['items']:
            icao_list.append(airport['icao'])
            airport_name_list.append(airport['name'])
            city_name_list.append(city)
            city_id_list.append(current_city['city_id'])

    airport_city_name_list = list(map(list, zip(airport_name_list, city_name_list, city_id_list)))
    airport_dict = dict(zip(icao_list, airport_city_name_list))

    return airport_dict


# get timestamps for next day
def get_timestamps():

    # get timestamp in correct format for tomorrow 8 AM until 8 PM
    date_tomorrow   = date.today() + timedelta(days=1)
    time_start      = time(hour=0, minute=0, second=0)
    time_middle     = time(hour=12, minute=0, second=0)
    time_end        = time(hour=23, minute=59, second=59)

    datetime_start = datetime.combine(date_tomorrow, time_start).strftime('%Y-%m-%dT%H:%M')
    datetime_middle = datetime.combine(date_tomorrow, time_middle).strftime('%Y-%m-%dT%H:%M')
    datetime_end = datetime.combine(date_tomorrow, time_end).strftime('%Y-%m-%dT%H:%M')

    return datetime_start, datetime_middle, datetime_end
    

# make api call to receive flight information
def get_flight_data(airport_dict, datetime_start, datetime_middle, datetime_end):
    
    # create time-list for looping through in order to get full 24h as aerobox api only allows windows of 12h
    time_list = [[datetime_start, datetime_middle], [datetime_middle, datetime_end]]
    
    flight_data_df_list = []

    for icao in airport_dict.keys():
        
        for times in time_list: 
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{times[0]}/{times[1]}"

            querystring = {"withLeg":"false","direction":"Arrival","withCancelled":"false","withCodeshared":"true","withCargo":"false","withPrivate":"false","withLocation":"false"}

            headers = {
                'X-RapidAPI-Key': 'X-RAPID-API-KEY',
                'X-RapidAPI-Host': 'aerodatabox.p.rapidapi.com'
            }

            response = requests.request("GET", url, headers=headers, params=querystring)
            
            # catch errors - if response code is not good, icao is skipped
            if response.status_code != 200:
                continue

            arrivals = response.json()

            # catch errors - if data received from api is empty, icao is skipped
            if not arrivals['arrivals']:
                continue
            
            print(icao, 'is being processed.')
            flight_data_df = pd.json_normalize(arrivals['arrivals']).sort_values(by='movement.scheduledTimeLocal')

            # standardize df
            cols_to_keep = ['number', 'movement.airport.icao', 'movement.airport.iata',
                'movement.airport.name', 'movement.scheduledTimeLocal', 'movement.scheduledTimeUtc', 
                'airline.name']
            flight_data_df = flight_data_df[cols_to_keep]

            flight_data_df.rename(columns= {'number':'flight_id', 'movement.airport.icao':'origin_icao', 'movement.airport.iata':'origin_iata',
                'movement.airport.name':'origin_airport', 'movement.scheduledTimeLocal':'scheduled_time_local', 'movement.scheduledTimeUtc':'scheduled_time_utc', 
                'airline.name':'airline'}, inplace=True)

            flight_data_df = (
                flight_data_df
                    .assign(arrival_airport_name = airport_dict[icao][0],
                            city_name = airport_dict[icao][1],
                            city_id = int(airport_dict[icao][2]))
            )    
            flight_data_df_list.append(flight_data_df)

    flight_data_df = pd.concat(flight_data_df_list)
    return flight_data_df


def lambda_handler(event, context):
    
    # choose city and corresponding country of interest
    city_country_dict = {'Berlin': 'Germany'}  

    # get connection to db
    con = connect_sql()
    
    # get cities df
    cities = get_cities(con)
    
    city_data = get_current_cities(city_country_dict, cities)
    datetime_start, datetime_middle, datetime_end = get_timestamps()
    forecast_data = get_weather(city_country_dict, city_data, datetime_start, datetime_end)
    airport_dict = get_airport_dict(city_country_dict, city_data)
    flight_data = get_flight_data(airport_dict, datetime_start, datetime_middle, datetime_end)
    
    flight_data.to_sql('flight_info', con=con, if_exists='append', index=False)
    forecast_data.to_sql('weather_info', con=con, if_exists='append', index=False)