package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpecieMapper extends Mapper<Object, Text, Text, NullWritable> {

    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] cols = value.toString().split(";");
        if (cols[3].equals("ESPECE"))
            return;
        context.write(new Text(cols[3]), NullWritable.get());
    }
}
