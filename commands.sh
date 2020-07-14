#!/bin/bash

# Start Hadoop
cd /usr/local/opt/hadoop/sbin
./start-dfs.sh

# Put file into HDFS
hadoop fs -put ~/Downloads/Accidents.csv /

#-----------------------------------------------#

# Number of Accidents Per Month
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q1.DriverClass /Accidents.csv /Q1_Output &&

# Number of Accidents vs Hour of Day
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q2.DriverClass /Accidents.csv /Q2_Output &&

# Number of Accidents Per Weekday
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q3.DriverClass /Accidents.csv /Q3_Output &&

# Percentage of Accidents per side of the road
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q4.DriverClass /Accidents.csv /Q4_Output &&

# Accidents Per State
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q5.DriverClass /Accidents.csv /Q5_Output &&

# Top 10 Accident Prone States
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q6.DriverClass /Q5_Output/part-r-00000 /Q6_Output &&

# State - Cities = Inverted Index
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q7.DriverClass /Accidents.csv /Q7_Output &&

# Average, Min and Max Temperature per Severity
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q8.DriverClass /Accidents.csv /Q8_Output &&

# Count of Accidents Per State Per Year (SecondarySorted with 5 Partitions - Per Year)
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q9.DriverClass /Accidents.csv /Q9_Output &&

# Effects of Wind Speed Per City, State - Recommendation System (RMS)
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q10.DriverClass /Accidents.csv /Q10_Output &&

# Divide file into partitions divided by state
hadoop fs -put ~/Downloads/State_Data.csv / &&
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q11.DriverClass /Accidents.csv /State_Data.csv /Q11_Output

# Percentage Per US Timezone
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q12.DriverClass /Accidents.csv /Q12_Output