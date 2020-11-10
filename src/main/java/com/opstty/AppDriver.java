package com.opstty;

import com.opstty.job.WordCount;
import com.opstty.job.District;
import com.opstty.job.Specie;

import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
                    
            programDriver.addClass("district", District.class,
                    "A MapReduce job that displays the list of district containing trees in this files");

            programDriver.addClass("specie", Specie.class,
                    "A MapReduce job that displays the list of Specie");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
