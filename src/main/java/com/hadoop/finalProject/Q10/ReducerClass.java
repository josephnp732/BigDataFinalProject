package com.hadoop.finalProject.Q10;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class ReducerClass extends Reducer<Text, Text, Text, Text> {

    private ArrayList<Text> mainList = new ArrayList<Text>();
    private ArrayList<Text> stateList = new ArrayList<Text>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        mainList.clear();
        stateList.clear();
        for (Text text : values) {
            if (text.charAt(0) == '@') {
                mainList.add(new Text(text.toString().substring(1)));
            } else if (text.charAt(0) == '#') {
                stateList.add(new Text(text.toString().substring(1)));
            }
        }
        executeJoinLogic(context);
    }

    public void executeJoinLogic(Context context) throws IOException, InterruptedException {
        String joinType = context.getConfiguration().get("join.type");

        //INNER JOIN OPERATION
        if (joinType.equalsIgnoreCase("inner")) {
            if (!mainList.isEmpty() && !stateList.isEmpty()) {
                for (Text stateTuple : stateList) {
                    for (Text mainTuple : mainList) {
                        context.write(stateTuple, mainTuple);
                    }
                }
            }
        }
    }


}
