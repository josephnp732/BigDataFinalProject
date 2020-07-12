#!/bin/bash

# Start Hadoop
cd /usr/local/opt/hadoop/sbin
./start-dfs.sh

# Put file into HDFS
hadoop fs -put ~/Downloads /

# Execute (Number of Accidents Per Month)
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q1.DriverClass /Accidents.csv /Q1_Output

# Execute (Number of Accidents vs Hour of Day)
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q2.DriverClass /Accidents.csv /Q2_Output

# Execute (Number of Accidents Per Weekday)
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q3.DriverClass /Accidents.csv /Q3_Output

# Execute (Number of Accidents Per Year)
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q4.DriverClass /Accidents.csv /Q4_Output