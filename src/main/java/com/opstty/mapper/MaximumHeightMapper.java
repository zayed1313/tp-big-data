package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaximumHeightMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] cols = value.toString().split(";");
        if (cols[6].equals("HAUTEUR"))
            return;
        
        IntWritable height = new IntWritable();
        try {
            height.set( (int) (Float.parseFloat(cols[6])));
        } catch (Exception e) {
            height.set(0);
        }

        context.write(new Text(cols[2]), height);
    }
}
