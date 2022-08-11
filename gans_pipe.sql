DROP DATABASE gans_pipe;
CREATE DATABASE IF NOT EXISTS gans_pipe; 

USE gans_pipe;

drop table if exists arrivals; 
create table if not exists arrivals(
	arrivals_id int auto_increment, 
    dep_airport text, 
    sched_arr_loc_time datetime, 
    terminal text, 
    status text, 
	aircraft text, 
    icao_code varchar(4),
    primary key (arrivals_id) #, 
    #foreign key (icao_code) references airports(icao_code)
);
 select * from arrivals;

drop table if exists airports;
create table if not exists airports(
	name text, 
    latitude_deg float, 
    longitude_deg float, 
    iso_country varchar(10), 
    iso_region varchar(10),
    municipality text, 
    icao_code varchar(4), 
    iata_code varchar(6), 
    municipality_iso_country varchar(200),
    primary key(icao_code) #,
    #foreign key (municipality_iso_country) references cities(municipality_iso_country)
);
-- select * from airports;

DROP TABLE IF EXISTS cities;
CREATE TABLE IF NOT EXISTS cities (
    city VARCHAR(200),
    mayor TEXT,
    city_size TEXT, 
    elevation TEXT, 
    city_population TEXT, 
    urban_population TEXT, 
    metro_population TEXT, 
    latitude TEXT, 
    longitude TEXT, 
	municipality_iso_country varchar(200),
    PRIMARY KEY(municipality_iso_country)
); 

-- select * from cities;
-- insert into cities(city, mayor) values ('WBS Coding School', 'test');
# if we run this query twice we will have an error with duplicated id

drop table if exists weather; 
create table if not exists weather (
	weather_id int auto_increment, 
    datetime datetime, 
    temperature float, 
    wind float, 
    prob_perc float, 
    rain_qty float, 
    snow integer, 
    municipality_iso_country varchar(200),
    primary key(weather_id) #, 
    #foreign key (municipality_iso_country) references cities(municipality_iso_country)
);
 select * from weather;


