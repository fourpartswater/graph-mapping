Run scripts for processing graphs
=================================

To process graphs, there is, in essence, a pipeline of pipelines. The processing scripts must be run in the following order:

 * convert.sh - this script requires some customization based on the dataset - specification of which columns correspond to which
   aspects of the data, or whether or not the graph is weighted or bidirectional.  See the case statement near the top of the 
   script for examples.
 * cluster.sh
 * layout.sh
 * tiling (any order):
   * node-tiling.sh
   * inter-edge-tiling.sh
   * intra-edge-tiling.sh
   * metadata-tiling.sh

All scripts expect their dataset to be based in a sub-directory of the place from which they are run, and intermediate files 
should be left as is - future scripts use them to do things like determine the number of hierarchy levels.  For each script, 
the dataset is specified with a '-d' or '--dataset' option, which should list the name of the subdirectory.

Also note, at the moment, that the base HDFS location in each script (specified by the BASE_LOCATION variable) is hard-coded
to my own directory in HDFS; this will change in future versions, but should of course be edited by the user until it is 
changed.

Tiling scripts have two additional parameters: -t and -b.  -t specifies the number of tile levels used to show the top level 
of the cluster hierarchy (the level with the fewest nodes).  -b specifies the number of tile levels used to show each of the 
lower levels of the cluster hierarchy.