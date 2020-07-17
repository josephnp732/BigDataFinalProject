package com.hadoop.finalProject.Q9;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableClass implements Writable {

    private double averageWindSpeed;
    private long totalCount;
    private double rms;

    public WritableClass() {
        super();
    }

    public WritableClass(double averageWindSpeed, long totalCount, double rms) {
        super();
        this.averageWindSpeed = averageWindSpeed;
        this.totalCount = totalCount;
        this.rms = rms;
    }

    @Override
    public String toString() {
        return "\tAverage WindSpeed = " + String.format("%.2f", averageWindSpeed) + "mph | RMS = " + String.format("%.2f", rms) + "mph";
    }

    public double getAverageWindSpeed() {
        return averageWindSpeed;
    }

    public void setAverageWindSpeed(double averageWindSpeed) {
        this.averageWindSpeed = averageWindSpeed;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public double getRms() {
        return rms;
    }

    public void setRms(double rms) {
        this.rms = rms;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(averageWindSpeed);
        dataOutput.writeLong(totalCount);
        dataOutput.writeDouble(rms);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        averageWindSpeed = dataInput.readDouble();
        totalCount = dataInput.readLong();
        rms = dataInput.readDouble();
    }
}
