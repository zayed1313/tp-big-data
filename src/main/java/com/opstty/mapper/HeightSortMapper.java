package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HeightSortMapper extends Mapper<Object, Text, IntWritable, Text> {

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] cols = value.toString().split(";");
        if (cols[1].equals("ARRONDISSEMENT"))
            return;
        
        IntWritable height = new IntWritable();
        try {
            height.set( (int) (Float.parseFloat(cols[6])));
        } catch (Exception e) {
            height.set(0);
        }
        
        context.write(height, new Text(cols[11]));
    }
}
