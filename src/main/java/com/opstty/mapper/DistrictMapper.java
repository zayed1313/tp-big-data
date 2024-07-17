package com.opstty.job;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader) {
            isHeader = false;
            return;
        }

        String[] fields = value.toString().split(";");
        if (fields.length > 1) {
            context.write(new Text(fields[1]), NullWritable.get());
        }
    }
}
