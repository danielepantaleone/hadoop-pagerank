Hadoop PageRank
===============

[PageRank](http://en.wikipedia.org/wiki/PageRank) algorithm implementation which make use of the 
[Apache Hadoop](http://hadoop.apache.org/) framework.

### Execute the program

* Install Hadoop on your machine [[OSX](http://shayanmasood.com/blog/how-to-setup-hadoop-on-mac-os-x-10-9-mavericks/)], [[Linux](http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/)] 
* Pick a dataset from the [Stanford web graphs](http://snap.stanford.edu/data/#web) collection
* Place the dataset in your Hadoop FS (example: */user/<your-nickname/dataset/input*)
* Create the directory which will contain the output (example: */user/<your-nickname/dataset/output*)
* Build a JAR using this source code and name it **pagerank.jar**
* Launch the software using Hadoop: `hadoop jar pagerank.jar --input <input-dir> --output <output-dir>`
* Browse the PageRank output result which can be found in the Hadoop FS (under the given output directory)

### Usage reference

* **--help** (*-h*): display the help text
* **--damping** (*-d*) <damping>: the damping factor [**OPTIONAL**] [**DEFAULT** = **0.85**]
* **--count** (*-c*) <iterations>: the amount of iterations [**OPTIONAL**] [**DEFAULT** = **2**]
* **--input** (*-i*) <input-dir>: the directory of the input graph [**REQUIRED**]
* **--output** (*-o*) <output-dir>: the directory of the output result [**REQUIRED**]