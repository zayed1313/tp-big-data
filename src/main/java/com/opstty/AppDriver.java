package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtTrees", DistrictTrees.class,
                    "A map/reduce program that display the list of distinct containing trees in csv file");
            programDriver.addClass("listTreeSpecies", ListTreeSpecies.class,
                    "A map/reduce program that returns the distinct tree species in csv file");
            programDriver.addClass("treeSpeciesCount", TreeSpeciesCount.class,
                    "A map/reduce program that returns the distinct tree species and the number of trees in csv file");
            programDriver.addClass("tallestTree", TallestTree.class,
                    "A map/reduce program that returns the tallest tree per species in csv file");
            programDriver.addClass("treesSortedByHeight", SortTreesByHeight.class,
                    "A map/reduce program that returns all the trees sorted by height.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
