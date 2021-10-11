package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class districMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(5);
    private Text word = new Text();



    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = (value.toString().split(";"));
        if(!data[0].equals("GEOPOINT")){
            //int treeID = Integer.parseInt(data[11]);
            int arrondissement = Integer.parseInt(data[1]);
            String str1 = Integer.toString(arrondissement);
            word.set(str1);
            context.write(word, one);
           /* StringTokenizer itr = new StringTokenizer(value.toString());
           /* while (itr.hasMoreTokens()) {
                //word.set(itr.nextToken());
                context.write(word, one);
            }*/
        }

    }
}
