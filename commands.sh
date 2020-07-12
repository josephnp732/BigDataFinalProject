#!/bin/bash

# Start Hadoop
cd /usr/local/opt/hadoop/sbin
./start-dfs.sh

# Put file into HDFS
hadoop fs -put ~/Downloads /

# Execute Question 1
hadoop jar /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar com.hadoop.finalProject.Q1.DriverClass /Accidents.csv /Q1_Output



