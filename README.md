# US Road Accidents Data Analysis (2016 - 2020) - Using Hadoop

#### Final Project for Engineering Big Data Systems - INFO 7250 (Summer 2020)

<b>Data Source:</b> </t> https://www.kaggle.com/sobhanmoosavi/us-accidents

#### <u>Pre-requisites:</u>

Have JDK 1.8, Hadoop, Hive & Pig installed in your local file system 

### MapReduce Question Set: 

`To run, execute ./commands.sh` 

1. Number of Accidents Per Month
2. Number of Accidents vs Hour of Day
3. Number of Accidents Per Day of the Week
4. Side of the Road Percentage
5. Number of Accidents Per State
6. Top 10 Accident Prone States (Filtering Techniques Top n Filtering Pattern)
7. State - Cities -> Inverted Index
8. Average, Min and Max Temperature per Severity
9. Count of Accidents Per State Per Year (SecondarySorted with 5 Partitions)
10. Divide file into partitions divided by state (Inner Join w/ Partitioner) [Execute on GCP DataProc]
11. Proximity to Traffic Object (Percentage / per all traffic)

### HIVE Question Set: (./hive)

`To execute hive file >> hive -f /Users/christy/Downloads/Projects/BigDataFinalProject/hive/queries.hql` 

1. Number of Accidents per timezone
2. Number of Accidents per Severity per State (Partitioning)
3. Top 10 - Weather Condition vs Number of. Accident

### Pig Question Set: (./pig)

`To run, in Pig Grunt shell >> run ./pig/q1.pig`  
`To run, in Pig Grunt shell >> run ./pig/q2.pig` 

1. Top 10 Weather Conditions during Accidents in Massachusetts (MA)
2. Cities in Massachusetts where accidents happen during snowy days (Replicated Inner Join)