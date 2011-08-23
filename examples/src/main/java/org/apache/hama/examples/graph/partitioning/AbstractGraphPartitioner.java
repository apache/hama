/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.examples.graph.partitioning;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hama.examples.graph.ShortestPaths;
import org.apache.hama.examples.graph.Vertex;

/**
 * This partitioner partitions the file data which should be in text form into a
 * sequencefile.
 * 
 * TODO: this should be extended with InputFormat stuff so we can parse every
 * format.
 * 
 */
public abstract class AbstractGraphPartitioner<T extends PartitionableWritable> {

  public static final Log LOG = LogFactory
      .getLog(AbstractGraphPartitioner.class);

  private FileSystem fs;

  private Class<T> vertexClass;

  @SuppressWarnings("unchecked")
  public Configuration partition(Configuration conf, Path file,
      String[] groomNames) throws InstantiationException,
      IllegalAccessException, IOException, InterruptedException {

    fs = FileSystem.get(conf);

    vertexClass = (Class<T>) conf.getClass("hama.partitioning.vertex.class",
        Vertex.class);

    int sizeOfCluster = groomNames.length;

    // setup the paths where the grooms can find their input
    List<Path> partPaths = new ArrayList<Path>(sizeOfCluster);
    List<SequenceFile.Writer> writers = new ArrayList<SequenceFile.Writer>(
        sizeOfCluster);
    StringBuilder groomNameBuilder = new StringBuilder();
    // this loop adds partition paths for the writers and sets the appropriate
    // groom names to the files and configuration
    for (String entry : groomNames) {
      partPaths.add(new Path(file.getParent().toString() + Path.SEPARATOR
          + ShortestPaths.PARTED + Path.SEPARATOR
          + entry.split(ShortestPaths.NAME_VALUE_SEPARATOR)[0]));
      conf.set(ShortestPaths.MASTER_TASK, entry);
      groomNameBuilder.append(entry + ";");
    }
    // put every peer into the configuration
    conf.set(ShortestPaths.BSP_PEERS, groomNameBuilder.toString());
    // create a seq writer for the files
    for (Path p : partPaths) {
      fs.delete(p, true);
      writers.add(SequenceFile.createWriter(fs, conf, p,
          ObjectWritable.class, ArrayWritable.class,CompressionType.NONE));
    }

    BufferedReader br = null;
    try {
      // read the input
      br = new BufferedReader(new InputStreamReader(fs.open(file)));

      long numLines = 0L;
      String line = null;
      while ((line = br.readLine()) != null) {
        // let the subclass process
        AdjacentPair<T> pair = process(line);
        // check to which partition the vertex belongs
        int mod = Math.abs(pair.vertex.getId() % sizeOfCluster);
        writers.get(mod).append(new ObjectWritable(vertexClass, pair.vertex),
            new ArrayWritable(vertexClass, pair.adjacentVertices));
        numLines++;
        if (numLines % 100000 == 0) {
          LOG.debug("Partitioned " + numLines + " of vertices!");
        }
      }

      for (Path p : partPaths) {
        conf.set("in.path." + p.getName(), p.toString());
      }
      conf.set("num.vertices", "" + numLines);
      LOG.debug("Partitioned a total of " + numLines + " vertices!");

      return conf;
    } finally {
      // close our ressources
      if (br != null)
        br.close();

      for (SequenceFile.Writer w : writers)
        w.close();

      fs.close();
    }
  }

  protected abstract AdjacentPair<T> process(String line);

}
