/**
 * Copyright (c) 2014 Daniele Pantaleone <danielepantaleone@icloud.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * 
 * @author Daniele Pantaleone
 * @version 1.0
 * @copyright Daniele Pantaleone, 17 October, 2014
 * @package it.uniroma1.hadoop.pagerank
 */

package it.uniroma1.hadoop.pagerank;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import it.uniroma1.hadoop.pagerank.job1.PageRankJob1Mapper;
import it.uniroma1.hadoop.pagerank.job1.PageRankJob1Reducer;
import it.uniroma1.hadoop.pagerank.job2.PageRankJob2Mapper;
import it.uniroma1.hadoop.pagerank.job2.PageRankJob2Reducer;
import it.uniroma1.hadoop.pagerank.job3.PageRankJob3Mapper;


public class PageRank {
    
    // args keys
    private static final String KEY_DAMPING = "--damping";
    private static final String KEY_DAMPING_ALIAS = "-d";
    
    private static final String KEY_COUNT = "--count";
    private static final String KEY_COUNT_ALIAS = "-c";
    
    private static final String KEY_INPUT = "--input";
    private static final String KEY_INPUT_ALIAS = "-i";
    
    private static final String KEY_OUTPUT = "--output";
    private static final String KEY_OUTPUT_ALIAS = "-o"; 
    
    private static final String KEY_HELP = "--help";
    private static final String KEY_HELP_ALIAS = "-h"; 
    
    // utility attributes
    public static NumberFormat NF = new DecimalFormat("00");
    public static Set<String> NODES = new HashSet<String>();
    public static String LINKS_SEPARATOR = "|";
    
    // configuration values
    public static Double DAMPING = 0.85;
    public static int ITERATIONS = 2;
    public static String IN_PATH = "";
    public static String OUT_PATH = "";
    
    
    /**
     * This is the main class run against the Hadoop cluster.
     * It will run all the jobs needed for the PageRank algorithm.
     */
    public static void main(String[] args) throws Exception {
        
        try {
            
            // parse input parameters
            for (int i = 0; i < args.length; i += 2) {
               
                String key = args[i];
                String value = args[i + 1];
                
                // NOTE: do not use a switch to keep Java 1.6 compatibility!
                if (key.equals(KEY_DAMPING) || key.equals(KEY_DAMPING_ALIAS)) {
                    // be sure to have a damping factor in the interval [0:1]
                    PageRank.DAMPING = Math.max(Math.min(Double.parseDouble(value), 1.0), 0.0);
                } else if (key.equals(KEY_COUNT) || key.equals(KEY_COUNT_ALIAS)) {
                    // be sure to have at least 1 iteration for the PageRank algorithm
                    PageRank.ITERATIONS = Math.max(Integer.parseInt(value), 1);
                } else if (key.equals(KEY_INPUT) || key.equals(KEY_INPUT_ALIAS)) {
                    PageRank.IN_PATH = value.trim();
                    if (PageRank.IN_PATH.charAt(PageRank.IN_PATH.length() - 1) == '/')
                        PageRank.IN_PATH = PageRank.IN_PATH.substring(0, PageRank.IN_PATH.length() - 1);
                } else if (key.equals(KEY_OUTPUT) || key.equals(KEY_OUTPUT_ALIAS)) {
                    PageRank.OUT_PATH = value.trim();
                    if (PageRank.OUT_PATH.charAt(PageRank.OUT_PATH.length() - 1) == '/')
                        PageRank.OUT_PATH = PageRank.OUT_PATH.substring(0, PageRank.IN_PATH.length() - 1);
                } else if (key.equals(KEY_HELP) || key.equals(KEY_HELP_ALIAS)) {
                    printUsageText(null);
                    System.exit(0);                        
                }
            }
            
        } catch (ArrayIndexOutOfBoundsException e) {
            printUsageText(e.getMessage());
            System.exit(1);
        } catch (NumberFormatException e) {
            printUsageText(e.getMessage());
            System.exit(1);
        }
        
        // check for valid parameters to be set
        if (PageRank.IN_PATH.isEmpty() || PageRank.OUT_PATH.isEmpty()) {
            printUsageText("missing required parameters");
            System.exit(1);
        }
        
        // delete output path if it exists already
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(new Path(PageRank.OUT_PATH)))
            fs.delete(new Path(PageRank.OUT_PATH), true);
        
        // print current configuration in the console
        System.out.println("Damping factor: " + PageRank.DAMPING);
        System.out.println("Number of iterations: " + PageRank.ITERATIONS);
        System.out.println("Input directory: " + PageRank.IN_PATH);
        System.out.println("Output directory: " + PageRank.OUT_PATH);
        System.out.println("---------------------------");
        
        Thread.sleep(1000);
        
        String inPath = null;;
        String lastOutPath = null;
        PageRank pagerank = new PageRank();
        
        System.out.println("Running Job#1 (graph parsing) ...");
        boolean isCompleted = pagerank.job1(IN_PATH, OUT_PATH + "/iter00");
        if (!isCompleted) {
            System.exit(1);
        }
        
        for (int runs = 0; runs < ITERATIONS; runs++) {
            inPath = OUT_PATH + "/iter" + NF.format(runs);
            lastOutPath = OUT_PATH + "/iter" + NF.format(runs + 1);
            System.out.println("Running Job#2 [" + (runs + 1) + "/" + PageRank.ITERATIONS + "] (PageRank calculation) ...");
            isCompleted = pagerank.job2(inPath, lastOutPath);
            if (!isCompleted) {
                System.exit(1);
            }
        }
        
        System.out.println("Running Job#3 (rank ordering) ...");
        isCompleted = pagerank.job3(lastOutPath, OUT_PATH + "/result");
        if (!isCompleted) {
            System.exit(1);
        }
        
        System.out.println("DONE!");
        System.exit(0);
    }
    
    /**
     * This will run the Job #1 (Graph Parsing).
     * Will parse the graph given as input and initialize the page rank.
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job1(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #1");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob1Mapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob1Reducer.class);
        
        return job.waitForCompletion(true);
     
    }
    
    /**
     * This will run the Job #2 (Rank Calculation).
     * It calculates the new ranking and generates the same output format as the input, 
     * so this job can run multiple times (more iterations will increase accuracy).
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job2(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #2");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob2Mapper.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankJob2Reducer.class);

        return job.waitForCompletion(true);
        
    }
    
    /**
     * This will run the Job #3 (Rank Ordering).
     * It will sort documents according to their page rank value.
     * 
     * @param in the directory of the input data
     * @param out the main directory of the output
     */
    public boolean job3(String in, String out) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
        Job job = Job.getInstance(new Configuration(), "Job #3");
        job.setJarByClass(PageRank.class);
        
        // input / mapper
        FileInputFormat.setInputPaths(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankJob3Mapper.class);
        
        // output
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true);
        
    }
    
    /**
     * Print the main an only help text in the System.out
     * 
     * @param err an optional error message to display
     */
    public static void printUsageText(String err) {
        
        if (err != null) {
            // if error has been given, print it
            System.err.println("ERROR: " + err + ".\n");
        }
       
        System.out.println("Usage: pagerank.jar " + KEY_INPUT + " <input> " + KEY_OUTPUT + " <output>\n");
        System.out.println("Options:\n");
        System.out.println("    " + KEY_INPUT + "    (" + KEY_INPUT_ALIAS + ")    <input>       The directory of the input graph [REQUIRED]");
        System.out.println("    " + KEY_OUTPUT + "   (" + KEY_OUTPUT_ALIAS + ")    <output>      The directory of the output result [REQUIRED]");
        System.out.println("    " + KEY_DAMPING + "  (" + KEY_DAMPING_ALIAS + ")    <damping>     The damping factor [OPTIONAL]");
        System.out.println("    " + KEY_COUNT + "    (" + KEY_COUNT_ALIAS + ")    <iterations>  The amount of iterations [OPTIONAL]");
        System.out.println("    " + KEY_HELP + "     (" + KEY_HELP_ALIAS + ")                  Display the help text\n");
    }
    
}