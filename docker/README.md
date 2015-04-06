# Hama Dockerfile #

This directory contains Dockerfile of [Hama](http://hama.apache.org) for [Docker](https://www.docker.com/)'s.


## What is Hama?
pache Hama is a general BSP computing engine on top of Hadoop, which was established in 2012 as a Top-Level Project of The Apache Software Foundation. It provides High-Performance computing engine for performing massive scientific and iterative algorithms on existing open source or enterprise Hadoop cluster, such as Matrix, Graph, and Machine Learning.[detail. https://hama.apache.org/](https://hama.apache.org/)


## Installation

1. Install [Docker](https://www.docker.com/).

2a. Build from files in this directory:
Review hama version inside The Dockerfile 'ENV HAMA_VERSION'

	$docker build -t <new name for image> .
        ex) $docker build -t hama-docker .

2b. Download automated build from public hub registry:
Pre-built image will down load and then you can run HAMA immediately.

	$docker pull hoseog/hama-docker


## Usage
You can run hama with docker command and let you go into container.

	$docker run -it -name <container name> -p 40013:40013 <image name>/bin/bash 
        ex) docker run -it -name hama-docker -p 40013:40013 hama-docker /bin/bash

Hama is located in /opt/hama/ and is almost ready to run.
Review configuration in /opt/hama/conf/ and you can start BST works.

