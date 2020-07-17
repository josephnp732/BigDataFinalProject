package com.hadoop.finalProject.Q8;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TempWritable implements Writable {

    private double maxTemp;
    private double averageTemp;
    private double minTemp;

    public double getMinTemp() {
        return minTemp;
    }

    public void setMinTemp(double minTemp) {
        this.minTemp = minTemp;
    }

    private long count;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double getMaxTemp() {
        return maxTemp;
    }

    public void setMaxTemp(double maxTemp) {
        this.maxTemp = maxTemp;
    }

    public double getAverageTemp() {
        return averageTemp;
    }

    public void setAverageTemp(double averageTemp) {
        this.averageTemp = averageTemp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(averageTemp);
        dataOutput.writeDouble(maxTemp);
        dataOutput.writeDouble(minTemp);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        averageTemp = dataInput.readDouble();
        maxTemp = dataInput.readDouble();
        minTemp = dataInput.readDouble();
        count = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "Max Temperature = " + maxTemp +
                " | Min Temperature = " + minTemp +
                " | Average Temperature = " + averageTemp;
    }
}
