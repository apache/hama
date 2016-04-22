# Apache Hama

<img src="http://hama.apache.org/images/hama_paint_logo.png" width="130" align="left"> Apache Hama is a framework for Big Data analytics which uses the Bulk Synchronous Parallel (BSP) computing model, which was established in 2012 as a Top-Level Project of The Apache Software Foundation.

It provides not only pure BSP programming model but also SQL-like query interface (Apache MRQL) and vertex/neuron centric programming models, inspired by Google's Pregel and DistBelief (Apache Horn). For the latest information about Hama, please visit our website at: <https://hama.apache.org/> and our wiki at: <https://wiki.apache.org/hama/>

## Getting Started

Please refer to the [Installation Guide](http://wiki.apache.org/hama/GettingStarted) in the online documentation for an overview on how to getting started with Hama.

## Run Examples

Hama provides examples package that allows you to quickly run examples on your Hama Cluster. To run one of them, use `% $HAMA_HOME/bin/hama jar hama-examples-x.x.x.jar`. For example:

Download a [Iris dataset](http://people.apache.org/~edwardyoon/kmeans.txt). And then, run K-Means using:

`% $HAMA_HOME/bin/hama jar hama-examples-x.x.x.jar kmeans /tmp/kmeans.txt /tmp/result 10 3`

## Getting Involved

Hama is an open source volunteer project under the Apache Software Foundation. We encourage you to learn about the project and contribute your expertise. 
