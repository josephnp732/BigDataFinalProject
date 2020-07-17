package com.hadoop.finalProject.Q9;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyComparator extends WritableComparator {

    public NaturalKeyComparator() {
        super(CompositeKeyWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKeyWritable w1 = (CompositeKeyWritable) a;
        CompositeKeyWritable w2 = (CompositeKeyWritable) b;

        return w1.getState().compareTo(w2.getState());

    }
}
