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
 * @package it.uniroma1.hadoop.pagerank.job2
 */

package it.uniroma1.hadoop.pagerank.job2;

import it.uniroma1.hadoop.pagerank.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankJob2Reducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
        
        /* PageRank calculation algorithm (reducer)
         * Input file format has 2 kind of records (separator is TAB):
         * 
         * One record composed by the collection of links of each page:
         * 
         *     <title>   |<link1>,<link2>,<link3>,<link4>, ... , <linkN>
         *     
         * Another record composed by the linked page, the page rank of the source page 
         * and the total amount of out links of the source page:
         *
         *     <link>    <page-rank>    <total-links>
         */
        
        String links = "";
        double sumShareOtherPageRanks = 0.0;
        
        for (Text value : values) {
 
            String content = value.toString();
            
            if (content.startsWith(PageRank.LINKS_SEPARATOR)) {
                // if this value contains node links append them to the 'links' string
                // for future use: this is needed to reconstruct the input for Job#2 mapper
                // in case of multiple iterations of it.
                links += content.substring(PageRank.LINKS_SEPARATOR.length());
            } else {
                
                String[] split = content.split("\\t");
                
                // extract tokens
                double pageRank = Double.parseDouble(split[0]);
                int totalLinks = Integer.parseInt(split[1]);
                
                // add the contribution of all the pages having an outlink pointing 
                // to the current node: we will add the DAMPING factor later when recomputing
                // the final pagerank value before submitting the result to the next job.
                sumShareOtherPageRanks += (pageRank / totalLinks);
            }

        }
        
        double newRank = PageRank.DAMPING * sumShareOtherPageRanks + (1 - PageRank.DAMPING);
        context.write(key, new Text(newRank + "\t" + links));
        
    }

}
