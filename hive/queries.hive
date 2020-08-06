-- Create Table
CREATE TABLE accidents(ID STRING, Source STRING, TMC DOUBLE, Severity INT, Start_Time STRING, End_Time STRING, Start_Lat DOUBLE, Start_Lng DOUBLE, End_Lat DOUBLE, End_Lng DOUBLE, Distance DOUBLE, Description STRING, Number DOUBLE, Street STRING, Side STRING, City STRING, County STRING, State STRING, Zipcode STRING, Country STRING, Timezone STRING, Airport_Code STRING, Weather_Timestamp STRING, Temperature DOUBLE, Wind_Chill DOUBLE,Humidity DOUBLE, Pressure DOUBLE, Visibility DOUBLE, Wind_Direction STRING, Wind_Speed DOUBLE, Precipitation DOUBLE, Weather_Condition STRING, Amenity BOOLEAN, Bump BOOLEAN, Crossing BOOLEAN, Give_Way BOOLEAN, Junction BOOLEAN, No_Exit BOOLEAN, Railway BOOLEAN, Roundabout BOOLEAN, Station BOOLEAN, Stop BOOLEAN, Traffic_Calming BOOLEAN, Traffic_Signal BOOLEAN, Turning_Loop BOOLEAN, Sunrise_Sunset STRING, Civil_Twilight STRING, Nautical_Twilight STRING, Astronomical_Twilight STRING) row format delimited fields terminated by ',';

-- Load Data into Table from Hive
LOAD DATA INPATH '/Accidents_hive.csv' OVERWRITE INTO TABLE accidents;

---------------------------------------------------------------------

-- Number of Accidents per timezone
SELECT timezone, count(*) from accidents WHERE timezone is not NULL AND timezone <> '' AND length(timezone) > 0 GROUP BY timezone;

---------------------------------------------------------------------

-- Number of Accidents per Severity per State (Partitioning)

CREATE TABLE IF NOT EXISTS state_part(city string,severity string) PARTITIONED BY(state string);

SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE state_part PARTITION(state) SELECT city,severity,state from accidents;

SELECT state, severity, count(*) from state_part WHERE severity is not NULL GROUP BY state, severity ORDER BY state DESC;

---------------------------------------------------------------------

-- Top 10 - Weather Condition vs Number of. Accident
SELECT weather_condition, count(*) as cnt from accidents GROUP BY weather_condition HAVING weather_condition <> '' ORDER BY cnt DESC LIMIT 10;

