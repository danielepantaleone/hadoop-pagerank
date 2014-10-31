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
 * @package it.uniroma1.hadoop.pagerank.job1
 */

package it.uniroma1.hadoop.pagerank.job1;

import it.uniroma1.hadoop.pagerank.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankJob1Reducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        /* Job#1 reducer will scroll all the nodes pointed by the given "key" node, constructing a
         * comma separated list of values and initializing the page rank for the "key" node.
         * Output format is the following (separator is TAB):
         * 
         *     <title>    <page-rank>    <link1>,<link2>,<link3>,<link4>,...,<linkN>
         *     
         * As for the pagerank initial value, early version of the PageRank algorithm used 1.0 as default, 
         * however later versions of PageRank assume a probability distribution between 0 and 1, hence the 
         * initial valus is set to DAMPING FACTOR / TOTAL NODES for each node in the graph.   
         */
        
        boolean first = true;
        String links = (PageRank.DAMPING / PageRank.NODES.size()) + "\t";

        for (Text value : values) {
            if (!first) 
                links += ",";
            links += value.toString();
            first = false;
        }

        context.write(key, new Text(links));
    }

}
