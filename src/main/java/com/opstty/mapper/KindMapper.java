package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KindMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();



    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] data = (value.toString().split(";"));
        if(!data[0].equals("GEOPOINT")){
            //int treeID = Integer.parseInt(data[11]);
            String espece = data[2];
            word.set(espece);
            context.write(word, one);
           /* StringTokenizer itr = new StringTokenizer(value.toString());
           /* while (itr.hasMoreTokens()) {
                //word.set(itr.nextToken());
                context.write(word, one);
            }*/
        }

    }
}
