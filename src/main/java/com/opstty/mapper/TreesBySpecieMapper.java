package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreesBySpecieMapper extends Mapper<Object, Text, Text, IntWritable> {

        IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] cols = value.toString().split(";");
        if (cols[2].equals("GENRE"))
            return;
        context.write(new Text(cols[2]), one );
    }
}