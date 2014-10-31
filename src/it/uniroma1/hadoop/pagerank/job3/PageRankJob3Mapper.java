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
 * @copyright Daniele Pantaleone, 19 October, 2014
 * @package it.uniroma1.hadoop.pagerank.job3
 */

package it.uniroma1.hadoop.pagerank.job3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankJob3Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        /* Rank Ordering (mapper only)
         * Input file format (separator is TAB):
         * 
         *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,... ,<linkN>
         * 
         * This is a simple job which does the ordering of our documents according to the computed pagerank.
         * We will map the pagerank (key) to its value (page) and Hadoop will do the sorting on keys for us.
         * There is no need to implement a reducer: the mapping and sorting is enough for our purpose.
         */
        
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        
        // extract tokens from the current line
        String page = Text.decode(value.getBytes(), 0, tIdx1);
        float pageRank = Float.parseFloat(Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1)));
        
        context.write(new DoubleWritable(pageRank), new Text(page));
        
    }
       
}
