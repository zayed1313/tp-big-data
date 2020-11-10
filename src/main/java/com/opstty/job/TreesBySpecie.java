package com.opstty.job;

import com.opstty.mapper.TreesBySpecieMapper;
import com.opstty.reducer.TreesBySpecieReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TreesBySpecie {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TreesBySpecie");
        job.setJarByClass(TreesBySpecie.class);
        job.setMapperClass(TreesBySpecieMapper.class);
        job.setCombinerClass(TreesBySpecieReducer.class);
        job.setReducerClass(TreesBySpecieReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}