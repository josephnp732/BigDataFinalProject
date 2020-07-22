
-- Cities in Massachusetts where accidents happen during snowy days

-- Load Data
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

data = LOAD '/Accidents.csv' USING CSVLoader() AS (ID:CHARARRAY, Source:CHARARRAY, TMC:DOUBLE, Severity:INT, Start_Time:CHARARRAY, End_Time:CHARARRAY, Start_Lat:DOUBLE, Start_Lng:DOUBLE, End_Lat:DOUBLE, End_Lng:DOUBLE, Distance:DOUBLE, Description:CHARARRAY, Number:DOUBLE, Street:CHARARRAY, Side:CHARARRAY, City:CHARARRAY, County:CHARARRAY, State:CHARARRAY, Zipcode:CHARARRAY, Country:CHARARRAY, Timezone:CHARARRAY, Airport_Code:CHARARRAY, Weather_Timestamp:CHARARRAY, Temperature:DOUBLE, Wind_Chill:DOUBLE, Humidity:DOUBLE, Pressure:DOUBLE, Visibility:DOUBLE, Wind_Direction:CHARARRAY, Wind_Speed:DOUBLE, Precipitation:DOUBLE, Weather_Condition:CHARARRAY, Amenity:BOOLEAN, Bump:BOOLEAN, Crossing:BOOLEAN, Give_Way:BOOLEAN, Junction:BOOLEAN, No_Exit:BOOLEAN, Railway:BOOLEAN, Roundabout:BOOLEAN, Station:BOOLEAN, Stop:BOOLEAN, Traffic_Calming:BOOLEAN, Traffic_Signal:BOOLEAN, Turning_Loop:BOOLEAN, Sunrise_Sunset:CHARARRAY, Civil_Twilight:CHARARRAY, Nautical_Twilight:CHARARRAY, Astronomical_Twilight:CHARARRAY);

-----------------------------------------------------------------

-- Find rows in State = MA where it snowed during accidents
weather_data = FOREACH data GENERATE ID, State, Weather_Condition;

ma = FILTER weather_data BY State == 'MA' AND NOT Weather_Condition == '';

snowy = FILTER ma BY Weather_Condition == 'Light Snow' OR Weather_Condition == 'Snow';

-----------------------------------------------------------------

-- Find All cities in US
cities = FOREACH data GENERATE ID, City ;

-----------------------------------------------------------------

joined = JOIN cities BY ID, snowy BY ID USING 'replicated';

intermediate = FOREACH joined GENERATE City, State;

result = DISTINCT intermediate;

dump result;