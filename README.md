Hierarchical Graph Clustering and Visualization Pipeline
========================================================

`graph-mapping` is a pipeline for clustering and hierarchically visualizing graph data. The graph-mapping clustering algorithm is based on the Louvain modularity algorithm [[Blondel et al.]](http://iopscience.iop.org/article/10.1088/1742-5468/2008/10/P10008). The pipeline ingests edge data from a graph, clusters the nodes into hierarchical communities and lays out all the graph components (nodes, communities, and edges) for visualization.

- [Ingesting Data](#ingesting-data)
- [Available Pipeline Tasks](#available-pipeline-tasks)
- [Scripts](#scripts)
- [Building the graph-mapping Library](#building-the-graph-mapping-library)
- [Running the Pipeline](#running-the-Pipeline)

## Ingesting Data ##

At a minimum, you must provide the graph-mapping pipeline with a list of edges in your graph. Each edge in the list should contain a reference to its source and destination nodes.

Along with the edge data, you can also include:

- Edge weights
- Node information
- Metadata
- Aggregations

## Available Pipeline Tasks ##

The following graph-mapping pipeline tasks are available to process your graph data:

1. [Conversion](#conversion)
2. [Clustering](#clustering)
3. [Layout](#layout)
4. [Tiling](#tiling), which is a series of subtasks that separately tile the following graph components:
   - Nodes
   - Metadata
   - Intra-community edges
   - Inter-community edges
5. [Exporting Data](#exporting-data)

Not every task needs to be called. For example, the output of the [tiling](#tiling) task is only useful if you have a client to consume the results. If you want to use the data in another pipeline, use the [data export](#exporting-data) instead.

### Conversion ###

The conversion task transforms your input data to the binary format required for [clustering](#clustering). Parameters are available to extract fields, renumber IDs and process metadata.

**NOTE**: The conversion task supports local files only.

### Clustering ###

The clustering task consumes data that has been output from the conversion task. Based on the Louvain clustering algorithm [[Blondel et al.]](http://iopscience.iop.org/article/10.1088/1742-5468/2008/10/P10008), it uses your graph's edge information to cluster nodes into hierarchical communities.

**NOTE**: The clustering algorithm runs on a single instance. If your data is too large, *Out of Memory* exceptions may be generated.

#### Output ####

Clustered data is output as a series of level outputs, where the level 0&mdash;the leaf level and most "zoomed in" view of your data&mdash;represents your raw nodes mapped to their communities. Each subsequent higher level represents an increasingly "zoomed out" view that clusters more and more nodes together. Each level is output to a single file containing both node and edge data.

As the data is clustered, analytics can be applied to aggregate data for communities. You can choose from a series of aggregations included in the library or your own create custom aggregations. Once created, aggregations can be specified via parameters.

Within a community, the node with the highest degree (most connections) becomes the community node. Each level output reuses the community node's information, including the ID.

##### Columns #####

The node data output has at least five columns in the following order:

1. File part ("node")
2. ID
3. Parent ID
4. Number of internal nodes
5. Weight
6. Metadata and analytic data (optional)

The edge data output has the following four columns:

1. File part ("edge")
2. Source ID
3. Destination ID
4. Weight

### Layout ###

The layout task outputs coordinate information for nodes and edges, along with the necessary clustering information and the specified metadata.

Clustered data is laid out using a top-down approach. At each level, a force-directed layout process determines the node positions of a community's members within its bounds.

#### Output ####

Like the clustering task, the layout output is split up into levels.

##### Columns #####

The node data has at least twelve columns:

1. File part ("node")
2. ID
3. x coordinate
4. y coordinate
5. Radius
6. Parent ID
7. Parent x coordinate
8. Parent y coordinate
9. Parent radius
10. Number of internal nodes
11. Degree
12. Level

Any metadata in the source input is added as additional columns at the end of the record.

### Tiling ###

The optional tiling task uses [Apache Spark](https://spark.apache.org/) to tile clustered data output from the [layout](#layout) task. Each level of data is mapped to one or more tile levels. Tiles can be stored in [Amazon S3](https://aws.amazon.com/s3/), [Apache HBase](https://hbase.apache.org/) or your local file system.

#### Output ####

Tiles are organized into folders and files by zoom level, column and row using the following TMS indexing:

```
{zoom_level}/{x}/{y}.bin
````

The tiles are binaries consisting of 64-bit floating point values arranged in row/major order. The row and column count are equal. Each bin element represents the count of the data values it contains.

| Tile Type | Description |
|:----------|:------------|
| Nodes | Contain information on nodes (communities). They are essentially a heatmap of the nodes, split off into tiles for every level. |
| Metadata | Used to add an informational layer to the graph. It contains basic community information (position, radius) as well as metadata present in the layout output. |
| Intra&#8209;community&nbsp;edges | Edges where both ends terminate within the same parent community. |
| Inter&#8209;community&nbsp;edges | Edges where each end terminates in a different parent community.  |

### Exporting Data ###

The data export task uses [Spark](https://spark.apache.org/) to add more information to the layout output and allows you to store the data in a format that is easier to consume. The output is divided into two directories: nodes and edges.

The exported data has two additional fields: `unique ID` and `community hierarchy`. Each node and edge can be uniquely identified, and the community hierarchy contains the path to the root node.

##### Columns #####

The node output has the following 13 columns:

- File part ("node")
- ID (unique across the whole dataset, as it incorporates the level)
- x coordinate
- y coordinate
- Radius
- Parent ID (unique across the whole dataset, as it incorporates the level)
- Parent x coordinate
- Parent y coordinate
- Parent radius
- Number of internal nodes
- Degree
- Level
- Hierarchy (Path to the root from the current node. For example, `12|15_c_0|15_c_1|7_c_2` denotes that node 12 has 15\_c\_0 as parent, which has 15\_c\_1 as parent, which has 7\_c\_2 as root community.

All metadata contained in the layout output is appended to end of each record.

The edge output has the following 11 columns:

- File part ("edge")
- Source node ID (Unique across the whole dataset, as it incorporates the level)
- Source node x coordinate
- Source node y coordinate
- Destination node ID (Unique across the whole dataset, as it incorporates the level)
- Destination node x coordinate
- Destination node y coordinate
- Edge weight
- 1 (if the edge is between two nodes having two different parents (inter community))
- Level
- Edge ID (Unique ID for the edge that combines the source and destination node IDs)

## Scripts ##

graph-mapping contains the following scripts that facilitate execution of the pipeline. They also serve as examples if you need to perform further integration or customization.

- [`convert.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/convert.sh)
- [`cluster.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/cluster.sh)
- [`layout.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/layout.sh)
- [`node-tiling.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/node-tiling.sh)
- [`metadata-tiling.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/metadata-tiling.sh)
- [`intra-edge-tiling.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/intra-edge-tiling.sh)
- [`inter-edge-tiling.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/inter-edge-tiling.sh)
- [`extract-es-data.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/extract-es-data.sh)

For each script, you must, at a minimum, specify a dataset using the `-d` parameter. The scripts take the dataset name and any optional parameters you specify and then execute the pipeline.

### Example ###

[`example.sh`](https://github.com/unchartedsoftware/graph-mapping/blob/master/src/scripts/example.sh) is an end-to-end script that executes every step of the pipeline. It uses [USPTO sample data](https://s3.ca-central-1.amazonaws.com/tiling-examples/patent-sample.zip) where:

- Each node represents a patent. Node data includes an arbitrary sequential ID column, the patent ID that the USPTO uses and the timestamp of the patent grant.
- Each edge represents a reference between patents. The edge data includes the source patent ID, the destination patent ID and the weight (1).

Pipeline tasks run sequentially, only relying on your local file system and Spark. Depending on the nature of your Spark instance, running the script may take around an hour, with most of the time spent in the tiling tasks.

## Building the graph-mapping Library ##

### Prerequisites ###

Before you begin, make sure you have installed the following third-party tools required to build the library and run tests:

- Java 1.7+
- Scala 2.11+

 **NOTE**: Because graph-mapping uses the Gradle wrapper, you do not need to install Gradle.

Additionally, build and install the following Uncharted projects according to the instructions in the respective repositories. These artifacts are currently not available in public Maven repositories and must be built from the source.

- [`sparkpipe-text-analytics`](https://github.com/unchartedsoftware/sparkpipe-text-analytics)
- [`salt-tiling-contrib`](https://github.com/unchartedsoftware/salt-tiling-contrib)

### Building the Library Binary ###

To build the graph-mapping library binary and install it locally:

1. Check out the source code.
2. Execute the following command in the project root:

   `./gradlew build install docs`

This will create a full distribution as .tar and .zip archives in `project_root/build/distributions`. The distribution consists of a single "fat" JAR that contains:

- Class binaries for the `graph-mapping` code
- All required dependencies
- Example configuration files and scripts that provide a starting point for running the graph pipeline

In addition to the distribution, a JAR consisting of `graph-mapping` class binaries only (suitable for inclusion as a dependency in other projects) will be available in `project_root/build/libs`.

To publish a full set of archives (binary, sources, test sources, docs) to a local Maven repository, execute the following command:

`./gradlew publish`

**NOTE**: Before you execute this command, make sure that `MAVEN_REPO_URL`, `MAVEN_REPO_USERNAME` and `MAVEN_REPO_PASSWORD` are defined as environment variables.

## Running the Pipeline ##

Before you begin, make sure you have installed Spark 2.0+ has been installed locally and that the `SPARK_HOME` environment variable is pointing to the installation directory. The examples can easily be adapted to run on a Spark cluster.

1. Starting in the project root directory, execute the following command to build the archive:

   ```bash
   ./gradlew build
   ```
2. Download the example USPTO data and unzip it in the run directory:

   ```bash
   cd src/scripts
   mkdir patent-sample
   wget https://s3.ca-central-1.amazonaws.com/tiling-examples/patent-sample.zip
   unzip patent-sample.zip
   ```

3. Run the job:

   ```bash
   ./example.sh
   ```

As the pipeline tasks are completed, graph-mapping writes the following files to `patent-sample/`:


| Task | Output |
|:-----|:-------|
| Conversion  | `edges.bin` and `metadata.bin` |
| Clustering  | `level_0` through `level_4` |
| Layout      | `layout` |
| Data export | `esexport` |