package com.hadoop.finalProject.Q1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ComparatorClass extends WritableComparator {

    SimpleDateFormat parser = new SimpleDateFormat("MMMMM");

    protected ComparatorClass() {
        super(Text.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        Text m1 = (Text) a;
        Text m2 = (Text) b;

        Date d1 = new Date();
        Date d2 = new Date();

        try {
            d1 = parser.parse(m1.toString());
            d2 = parser.parse(m2.toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return d1.compareTo(d2);
    }
}
