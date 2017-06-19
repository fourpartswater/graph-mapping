#Hierarchical Graph Clustering and Visualization Pipeline
`xdata-graph` provides a pipeline to cluster and visualize graph data using a hierarchical clustering algorithm based on the Louvain clustering algorithm [Blondel, Guillaume, and Lambiotte, at at]. Given the edge data representing a graph, the pipeline will cluster the graph into hierarchical communities and will then lay them out for display purposes.

the xdata-graph pipeline has 8 tasks that can used as needed to process the graph data. They are:

  - Convert
  - Cluster
  - Layout
  - Node Tile
  - Metadata Tile
  - Intra Community Edge Tile
  - Inter Community Edge Tile
  - Data Export

Not every task needs to be called. For example, tiling produces a specific output which is only useful if the client already exists to consume the results. If the data is to be tied into another pipeline, then the data export task is more likely to be useful.

##Data
The minimal data required for the graph pipeline is a list of edges, with each edge defining a source and destination node. Additional data can be defined, including:

  - Edge weight
  - Node information
  - Metadata
  - Aggregations

##Tasks
###Convert
The convert task takes the input data and converts it to the format necessary for clustering. Parameters are available to extract the right fields, renumber ids and process metadata. The convert task supports local files only.

###Cluster
Clustering is done on the output of the conversion task. It uses edge information to hierarchically cluster the data based on the Louvain clustering algorithm [Blondel, Guillaume, and Lambiotte, at at]. The clustered data is output as a series of level outputs with the lowest level having the raw node information mapped to their communities. Each output file contains edge & node information.

The node chosen as community node is the node within the community that has the highest degree. The level outputs will reuse the community nodes', information including the id.

As the data is clustered, analytics can be applied to aggregate data for communities. A series of aggregations are included in the library, and custom ones can be created. Once created, the aggregation is specified via parameters.

The clustering algorithm is run on a single instance. If the data is too large, Out of Memory exceptions could occur.

###Layout
The clustered data is laid out using a top down approach. At any level, a community's members are laid out within its bounds. A force directed layout process is used to determine node positions within the necessary bounds.

The layout step outputs coordinate information for nodes & edges, along with the necessary clustering information & the specified meatadata.

###Tiling
The layout output can be used to tile the clustered data. Each level of data is mapped to one or more tile level. Tiles can be stored to S3, HBase or local file system.

All tiling tasks use Spark to generate output.

###Node Tile
Node tiles contain information on nodes (communities). They are essentially a heatmap of the nodes, split off into tiles for every level.

###Metadata Tile
Metadata tiles are used to add an informational layer to the graph. It contains basic community information (position, radius) as well as metadata present in the layout output.

###Intra Community Edge Tile
Intra community edges are edges with both ends having the same parent community.

###Inter Community Edge Tile
Inter community edges are edges with the ends having different parent communities.

###Data Export
The data export task is used to add more information to the layout output and to store the data in a format that is easier to consume. The exported data has two additional fields: unique id & community hierarchy. Each node & edge can be uniquely identified, and the community hierarchy contains the path to the root node.

Spark is used to process the data and create the output.

##Scripts
A series of scripts are available as part of the repo. These scrips facilitate the execution of the pipeline. They also serve as examples if further integration or customization is required.

The main scripts are:

  - convert.sh
  - cluster.sh
  - layout.sh
  - node-tiling.sh
  - metadata-tiling.sh
  - intra-edge-tiling.sh
  - inter-edge-tiling.sh
  - extract-es-data.sh

At a minimum, the scripts require the dataset be specified via the -d parameter. The scripts will then take the dataset name (along with other optional parameters) and will execute the pipeline.

###Example
As an example of an end-to-end execution, example.sh executes every step of the pipeline. It uses a sample of data from the USPTO, with each node representing a patent and each edge representing a reference between patents. The node data has an arbitrary sequential id column, the patent id the USPTO uses and the timestamp of the patent grant. The edge data contains the source patent id, the destination patent id and the weight (1).

The tasks are run sequentially and only rely on the local file system and Spark. run the script could take around an hour, depending on the nature of the Spark instance. Most of that time will be spent in the tiling tasks.

##Building
Java 1.7+, Scala 2.11+ are required to build the library and run tests. The Gradle wrapper is used so there is no requirement to have Gradle installed.

As a pre-requisite, build and install the `sparkpipe-xdata-text` project following the instructions [here](https://github.com/unchartedsoftware/sparkpipe-xdata-text).  This artifact is currently not available in a public Maven repository and needs to be built from source.

As another pre-requisite, build and install the `xdata-salt-tiling` project following the instructions [here](https://github.com/unchartedsoftware/xdata-salt-tiling).  This artifact is currently not available in a public Maven repository and needs to be built from source.

After checking out the source code, the library binary can be built from the project root and installed locally as follows:

`./gradlew build install docs`

A full distribution will be available as tar and zip archives in the `project_root/build/distributions` directory. The distribution consists of a single "fat" JAR that contains the class binaries for the `xdata-graph` code, along with all dependencies it requires. The distribution also contains example configuration files and run scripts that provide a starting point for running the graph pipeline.

In addition to the distribution, a JAR consisting of `xdata-graph` class binaries only (suitable for inclusion as a dependency in other projects) will be available in `project_root/build/libs`, and a full set of archives (binary, sources, test sources, docs) can be published to a local Maven repository via:

`./gradlew publish`

Note that The above command requires that `MAVEN_REPO_URL`, `MAVEN_REPO_USERNAME` and `MAVEN_REPO_PASSWORD` be defined as environment variables.

##Running the Pipeline
The following instructions assume that `Spark 2.0+` has been installed locally, and that the SPARK_HOME environment variable is pointing to the installation directory. The examples can easily be adapted to run on a Spark cluster.

Starting in the project root directory, execute the following to build the archive:

```bash
./gradlew build
```

Download the example USPTO data and unzip into the run directory:

```bash
cd src/scripts
mkdir patent-sample
wget https://s3.ca-central-1.amazonaws.com/tiling-examples/patent-sample.zip
unzip patent-sample.zip
```

Run the job:

```bash
./example.sh
```

As the tasks complete, files will be written in the patent-sample folder:

  - `edges.bin` & `metadata.bin` are the outputs of conversion
  - `level_0` through `level_4` are the outputs of clustering
  - `layout` is the output of the layout step
  - `esexport` is the layout of the data export
