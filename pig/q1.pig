<<<<<<< HEAD

-- Number of Accidents per Weather_Condition per State

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

data = LOAD '/Accidents.csv' USING CSVLoader() AS (ID:CHARARRAY, Source:CHARARRAY, TMC:DOUBLE, Severity:INT, Start_Time:CHARARRAY, End_Time:CHARARRAY, Start_Lat:DOUBLE, Start_Lng:DOUBLE, End_Lat:DOUBLE, End_Lng:DOUBLE, Distance:DOUBLE, Description:CHARARRAY, Number:DOUBLE, Street:CHARARRAY, Side:CHARARRAY, City:CHARARRAY, County:CHARARRAY, State:CHARARRAY, Zipcode:CHARARRAY, Country:CHARARRAY, Timezone:CHARARRAY, Airport_Code:CHARARRAY, Weather_Timestamp:CHARARRAY, Temperature:DOUBLE, Wind_Chill:DOUBLE, Humidity:DOUBLE, Pressure:DOUBLE, Visibility:DOUBLE, Wind_Direction:CHARARRAY, Wind_Speed:DOUBLE, Precipitation:DOUBLE, Weather_Condition:CHARARRAY, Amenity:BOOLEAN, Bump:BOOLEAN, Crossing:BOOLEAN, Give_Way:BOOLEAN, Junction:BOOLEAN, No_Exit:BOOLEAN, Railway:BOOLEAN, Roundabout:BOOLEAN, Station:BOOLEAN, Stop:BOOLEAN, Traffic_Calming:BOOLEAN, Traffic_Signal:BOOLEAN, Turning_Loop:BOOLEAN, Sunrise_Sunset:CHARARRAY, Civil_Twilight:CHARARRAY, Nautical_Twilight:CHARARRAY, Astronomical_Twilight:CHARARRAY);

inter = FOREACH data GENERATE State, Weather_Condition ;

ma = FILTER inter BY State == 'MA' AND NOT Weather_Condition == '';

grouped = GROUP ma BY CONCAT (State, ' - ', Weather_Condition);

concat_count = FOREACH grouped GENERATE group, COUNT(ma) as cnt;

sorted = ORDER concat_count BY cnt DESC;

top = LIMIT sorted 10;

dump top;
=======
DATA = LOAD
>>>>>>> 13f734fa74a21bcbc8e4d7aa80bc04adbda3e901
