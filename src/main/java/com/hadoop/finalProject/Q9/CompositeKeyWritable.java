package com.hadoop.finalProject.Q9;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKeyWritable implements WritableComparable<CompositeKeyWritable> {

    private String state;
    private String year;

    @Override
    public String toString() {
        return "CompositeKeyWritable{" +
                "state='" + state + '\'' +
                ", year='" + year + '\'' +
                '}';
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public CompositeKeyWritable(String state, String weatherCondition) {
        super();
        this.state = state;
        this.year = weatherCondition;
    }

    public CompositeKeyWritable() {
        super();
    }

    @Override
    public int compareTo(CompositeKeyWritable o) {
        int result = this.state.compareTo(o.state);
        if(result == 0) {
            return this.year.compareTo(o.year);
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(state);
        dataOutput.writeUTF(year);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        state = dataInput.readUTF();
        year = dataInput.readUTF();
    }
}
