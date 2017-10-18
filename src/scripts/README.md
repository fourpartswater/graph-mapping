Run Scripts for Processing Graphs
=================================

Processing graphs requires a pipeline of scripts that must be run in the following order:

 1. `convert.sh` (Requires dataset-specific customization. Specify which columns correspond to which aspects of the data or indicate whether the graph is weighted or bidirectional.  See the case statement near the top of the script for examples.)
 2. `cluster.sh`
 3. `layout.sh`
 4. tiling (any order):
    - `node-tiling.sh`
    - `inter-edge-tiling.sh`
    - `intra-edge-tiling.sh`
    - `metadata-tiling.sh`

**To execute a script**:

1. Make sure that your dataset is located in a subdirectory of the folder in which you will run the script.

   **NOTE**: Leave intermediate files as is. Future scripts may use them to, for example, determine the number of hierarchy levels.

2. Edit the base HDFS location (`BASE_LOCATION`) in the script.
3. Specify the dataset by using the `-d` or `--dataset` flag to enter the name of the subdirectory.

   **NOTE**: Tiling scripts have two additional parameters:

   - `-t` specifies the number of tile levels used to show the top level of the cluster hierarchy (the level with the fewest nodes).
   - `-b` specifies the number of tile levels used to show each of the lower levels of the cluster hierarchy.