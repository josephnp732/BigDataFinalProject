package com.hadoop.finalProject.Q9;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

    protected CompositeKeyComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CompositeKeyWritable w1 = (CompositeKeyWritable) a;
        CompositeKeyWritable w2 = (CompositeKeyWritable) b;

        int result = w1.getYear().compareTo(w2.getYear());

        if(result == 0) {
            return w1.getState().compareTo(w2.getState());
        }

        return result;
    }

}
