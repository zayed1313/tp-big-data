package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SpeciesCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    public int current_line = 0;
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (current_line != 0) {
            context.write(new Text(value.toString().split(";")[3]), one);
        }
        current_line++;
    }
}