#!/bin/bash

# Package project to jar file
mvn package

# Variables (TODO: Change)
keyName="Lab2"
projectName="ebds-cluster"
bucketName="ebds-final-project"

# Upload jar file to S3 bucket
aws s3 cp /Users/christy/Downloads/Projects/BigDataFinalProject/target/BigDataFinalProject-1.0-SNAPSHOT.jar s3://$bucketName/ebds-runnable.jar

# Create a new Cluster
clusterId=$(aws emr create-cluster \
  --name $projectName \
  --use-default-roles \
  --release-label emr-5.30.1 \
  --instance-count 3 \
  --instance-type m5.xlarge \
  --applications Name=Pig Name=Hive Name=Hadoop \
  --ec2-attributes KeyName=$keyName  \
  --log-uri s3://$bucketName/logs \
  --auto-terminate | awk  '{print $2}')

# Add Steps to cluster
aws emr add-steps \
  --cluster-id $clusterId \
  --steps Type=CUSTOM_JAR,Name=Q1_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q1.DriverClass,s3://$bucketName/Accidents.csv,/Q1_Output \
  Type=CUSTOM_JAR,Name=Q2_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q2.DriverClass,s3://$bucketName/Accidents.csv,/Q2_Output \
  Type=CUSTOM_JAR,Name=Q3_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q3.DriverClass,s3://$bucketName/Accidents.csv,/Q3_Output \
  Type=CUSTOM_JAR,Name=Q4_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q4.DriverClass,s3://$bucketName/Accidents.csv,/Q4_Output \
  Type=CUSTOM_JAR,Name=Q5_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q5.DriverClass,s3://$bucketName/Accidents.csv,/Q5_Output \
  Type=CUSTOM_JAR,Name=Q6_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q6.DriverClass,s3://$bucketName/Accidents.csv,/Q5_Output/part-r-00000,/Q6_Output \
  Type=CUSTOM_JAR,Name=Q7_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q7.DriverClass,s3://$bucketName/Accidents.csv,/Q7_Output \
  Type=CUSTOM_JAR,Name=Q8_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q8.DriverClass,s3://$bucketName/Accidents.csv,/Q8_Output \
  Type=CUSTOM_JAR,Name=Q9_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q9.DriverClass,s3://$bucketName/Accidents.csv,/Q9_Output \
  Type=CUSTOM_JAR,Name=Q10_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q10.DriverClass,s3://$bucketName/Accidents.csv,s3://$bucketName/State_Data.csv,/Q10_Output \
  Type=CUSTOM_JAR,Name=Q11_Step,ActionOnFailure=CONTINUE,Jar=s3://$bucketName/ebds-runnable.jar,Args=com.hadoop.finalProject.Q11.DriverClass,s3://$bucketName/Accidents.csv,/Q11_Output

