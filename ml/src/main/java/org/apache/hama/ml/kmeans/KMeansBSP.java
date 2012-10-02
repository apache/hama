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
package org.apache.hama.ml.kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.ml.distance.DistanceMeasurer;
import org.apache.hama.ml.distance.EuclidianDistance;
import org.apache.hama.ml.math.DenseDoubleVector;
import org.apache.hama.ml.math.DoubleVector;
import org.apache.hama.ml.writable.VectorWritable;
import org.apache.hama.util.ReflectionUtils;

import com.google.common.base.Preconditions;

/**
 * K-Means in BSP that reads a bunch of vectors from input system and a given
 * centroid path that contains initial centers.
 * 
 */
public final class KMeansBSP
    extends
    BSP<VectorWritable, NullWritable, IntWritable, VectorWritable, CenterMessage> {

  public static final String CENTER_OUT_PATH = "center.out.path";
  public static final String MAX_ITERATIONS_KEY = "k.means.max.iterations";
  public static final String CACHING_ENABLED_KEY = "k.means.caching.enabled";
  public static final String DISTANCE_MEASURE_CLASS = "distance.measure.class";
  public static final String CENTER_IN_PATH = "center.in.path";

  private static final Log LOG = LogFactory.getLog(KMeansBSP.class);
  // a task local copy of our cluster centers
  private DoubleVector[] centers;
  // simple cache to speed up computation, because the algorithm is disk based
  private List<DoubleVector> cache;
  // numbers of maximum iterations to do
  private int maxIterations;
  // our distance measurement
  private DistanceMeasurer distanceMeasurer;
  private Configuration conf;

  @Override
  public final void setup(
      BSPPeer<VectorWritable, NullWritable, IntWritable, VectorWritable, CenterMessage> peer)
      throws IOException, InterruptedException {

    conf = peer.getConfiguration();

    Path centroids = new Path(peer.getConfiguration().get(CENTER_IN_PATH));
    FileSystem fs = FileSystem.get(peer.getConfiguration());
    final ArrayList<DoubleVector> centers = new ArrayList<DoubleVector>();
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(fs, centroids, peer.getConfiguration());
      VectorWritable key = new VectorWritable();
      NullWritable value = NullWritable.get();
      while (reader.next(key, value)) {
        DoubleVector center = key.getVector();
        centers.add(center);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }

    Preconditions.checkArgument(centers.size() > 0,
        "Centers file must contain at least a single center!");
    this.centers = centers.toArray(new DoubleVector[centers.size()]);


    String distanceClass = peer.getConfiguration().get(DISTANCE_MEASURE_CLASS);
    if (distanceClass != null) {
      try {
        distanceMeasurer = ReflectionUtils.newInstance(distanceClass);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(new StringBuilder("Wrong DistanceMeasurer implementation ").
            append(distanceClass).append(" provided").toString());
      }
    }
    else {
      distanceMeasurer = new EuclidianDistance();
    }

    maxIterations = peer.getConfiguration().getInt(MAX_ITERATIONS_KEY, -1);
    // normally we want to rely on OS caching, but if not, we can cache in heap
    if (peer.getConfiguration().getBoolean(CACHING_ENABLED_KEY, false)) {
      cache = new ArrayList<DoubleVector>();
    }
  }

  @Override
  public final void bsp(
      BSPPeer<VectorWritable, NullWritable, IntWritable, VectorWritable, CenterMessage> peer)
      throws IOException, InterruptedException, SyncException {
    long converged;
    while (true) {
      assignCenters(peer);
      peer.sync();
      converged = updateCenters(peer);
      peer.reopenInput();
      if (converged == 0)
        break;
      if (maxIterations > 0 && maxIterations < peer.getSuperstepCount())
        break;
    }
    LOG.info("Finished! Writing the assignments...");
    recalculateAssignmentsAndWrite(peer);
    LOG.info("Done.");
  }

  private long updateCenters(
      BSPPeer<VectorWritable, NullWritable, IntWritable, VectorWritable, CenterMessage> peer)
      throws IOException {
    // this is the update step
    DoubleVector[] msgCenters = new DoubleVector[centers.length];
    int[] incrementSum = new int[centers.length];
    CenterMessage msg;
    // basically just summing incoming vectors
    while ((msg = peer.getCurrentMessage()) != null) {
      DoubleVector oldCenter = msgCenters[msg.getCenterIndex()];
      DoubleVector newCenter = msg.getData();
      incrementSum[msg.getCenterIndex()] += msg.getIncrementCounter();
      if (oldCenter == null) {
        msgCenters[msg.getCenterIndex()] = newCenter;
      } else {
        msgCenters[msg.getCenterIndex()] = oldCenter.add(newCenter);
      }
    }
    // divide by how often we globally summed vectors
    for (int i = 0; i < msgCenters.length; i++) {
      // and only if we really have an update for c
      if (msgCenters[i] != null) {
        msgCenters[i] = msgCenters[i].divide(incrementSum[i]);
      }
    }
    // finally check for convergence by the absolute difference
    long convergedCounter = 0L;
    for (int i = 0; i < msgCenters.length; i++) {
      final DoubleVector oldCenter = centers[i];
      if (msgCenters[i] != null) {
        double calculateError = oldCenter.subtract(msgCenters[i]).abs().sum();
        if (calculateError > 0.0d) {
          centers[i] = msgCenters[i];
          convergedCounter++;
        }
      }
    }
    return convergedCounter;
  }

  private void assignCenters(
      BSPPeer<VectorWritable, NullWritable, IntWritable, VectorWritable, CenterMessage> peer)
      throws IOException {
    // each task has all the centers, if a center has been updated it
    // needs to be broadcasted.
    final DoubleVector[] newCenterArray = new DoubleVector[centers.length];
    final int[] summationCount = new int[centers.length];
    // if our cache is not enabled, iterate over the disk items
    if (cache == null) {
      // we have an assignment step
      final NullWritable value = NullWritable.get();
      final VectorWritable key = new VectorWritable();
      while (peer.readNext(key, value)) {
        assignCentersInternal(newCenterArray, summationCount, key.getVector()
            .deepCopy());
      }
    } else {
      // if our cache is enabled but empty, we have to read it from disk first
      if (cache.isEmpty()) {
        final NullWritable value = NullWritable.get();
        final VectorWritable key = new VectorWritable();
        while (peer.readNext(key, value)) {
          DoubleVector deepCopy = key.getVector().deepCopy();
          cache.add(deepCopy);
          // but do the assignment directly
          assignCentersInternal(newCenterArray, summationCount, deepCopy);
        }
      } else {
        // now we can iterate in memory and check against the centers
        for (DoubleVector v : cache) {
          assignCentersInternal(newCenterArray, summationCount, v);
        }
      }
    }
    // now send messages about the local updates to each other peer
    for (int i = 0; i < newCenterArray.length; i++) {
      if (newCenterArray[i] != null) {
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, new CenterMessage(i, summationCount[i],
              newCenterArray[i]));
        }
      }
    }
  }

  private void assignCentersInternal(final DoubleVector[] newCenterArray,
      final int[] summationCount, final DoubleVector key) {
    final int lowestDistantCenter = getNearestCenter(key);
    final DoubleVector clusterCenter = newCenterArray[lowestDistantCenter];
    if (clusterCenter == null) {
      newCenterArray[lowestDistantCenter] = key;
    } else {
      // add the vector to the center
      newCenterArray[lowestDistantCenter] = newCenterArray[lowestDistantCenter]
          .add(key);
      summationCount[lowestDistantCenter]++;
    }
  }

  private int getNearestCenter(DoubleVector key) {
    int lowestDistantCenter = 0;
    double lowestDistance = Double.MAX_VALUE;
    for (int i = 0; i < centers.length; i++) {
      final double estimatedDistance = distanceMeasurer.measureDistance(
          centers[i], key);
      // check if we have a can assign a new center, because we
      // got a lower distance
      if (estimatedDistance < lowestDistance) {
        lowestDistance = estimatedDistance;
        lowestDistantCenter = i;
      }
    }
    return lowestDistantCenter;
  }

  private void recalculateAssignmentsAndWrite(
      BSPPeer<VectorWritable, NullWritable, IntWritable, VectorWritable, CenterMessage> peer)
      throws IOException {
    final NullWritable value = NullWritable.get();
    // also use our cache to speed up the final writes if exists
    if (cache == null) {
      final VectorWritable key = new VectorWritable();
      IntWritable keyWrite = new IntWritable();
      while (peer.readNext(key, value)) {
        final int lowestDistantCenter = getNearestCenter(key.getVector());
        keyWrite.set(lowestDistantCenter);
        peer.write(keyWrite, key);
      }
    } else {
      IntWritable keyWrite = new IntWritable();
      for (DoubleVector v : cache) {
        final int lowestDistantCenter = getNearestCenter(v);
        keyWrite.set(lowestDistantCenter);
        peer.write(keyWrite, new VectorWritable(v));
      }
    }
    // just on the first task write the centers to filesystem to prevent
    // collisions
    if (peer.getPeerName().equals(peer.getPeerName(0))) {
      String pathString = conf.get(CENTER_OUT_PATH);
      if (pathString != null) {
        final SequenceFile.Writer dataWriter = SequenceFile.createWriter(
            FileSystem.get(conf), conf, new Path(pathString),
            VectorWritable.class, NullWritable.class, CompressionType.NONE);
        for (DoubleVector center : centers) {
          dataWriter.append(new VectorWritable(center), value);
        }
        dataWriter.close();
      }
    }
  }

  /**
   * Creates a basic job with sequencefiles as in and output.
   */
  public static BSPJob createJob(Configuration cnf, Path in, Path out,
      boolean textOut) throws IOException {
    HamaConfiguration conf = new HamaConfiguration(cnf);
    BSPJob job = new BSPJob(conf, KMeansBSP.class);
    job.setJobName("KMeans Clustering");
    job.setJarByClass(KMeansBSP.class);
    job.setBspClass(KMeansBSP.class);
    job.setInputPath(in);
    job.setOutputPath(out);
    job.setInputFormat(org.apache.hama.bsp.SequenceFileInputFormat.class);
    if (textOut)
      job.setOutputFormat(org.apache.hama.bsp.TextOutputFormat.class);
    else
      job.setOutputFormat(org.apache.hama.bsp.SequenceFileOutputFormat.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    return job;
  }

  public static void main(String[] args) throws IOException,
      ClassNotFoundException, InterruptedException {

    if (args.length < 6) {
      LOG.info("USAGE: <INPUT_PATH> <OUTPUT_PATH> <COUNT> <K> <DIMENSION OF VECTORS> <MAXITERATIONS> <optional: num of tasks>");
      return;
    }

    Configuration conf = new Configuration();
    int count = Integer.parseInt(args[2]);
    int k = Integer.parseInt(args[3]);
    int dimension = Integer.parseInt(args[4]);
    int iterations = Integer.parseInt(args[5]);
    conf.setInt(MAX_ITERATIONS_KEY, iterations);

    Path in = new Path(args[0]);
    Path out = new Path(args[1]);
    Path center = new Path(in, "center/cen.seq");
    Path centerOut = new Path(out, "center/center_output.seq");

    conf.set(CENTER_IN_PATH, center.toString());
    conf.set(CENTER_OUT_PATH, centerOut.toString());
    // if you're in local mode, you can increase this to match your core sizes
    conf.set("bsp.local.tasks.maximum", ""
        + Runtime.getRuntime().availableProcessors());
    // deactivate (set to false) if you want to iterate over disk, else it will
    // cache the input vectors in memory
    conf.setBoolean(CACHING_ENABLED_KEY, true);
    BSPJob job = createJob(conf, in, out, false);

    LOG.info("N: " + count + " k: " + k + " Dimension: " + dimension
        + " Iterations: " + iterations);

    FileSystem fs = FileSystem.get(conf);
    // prepare the input, like deleting old versions and creating centers
    prepareInput(count, k, dimension, conf, in, center, out, fs);
    if (args.length == 7) {
      job.setNumBspTask(Integer.parseInt(args[6]));
    }

    // just submit the job
    job.waitForCompletion(true);
  }

  /**
   * Reads the centers outputted from the clustering job.
   * 
   * @return an index on the key dimension, and a cluster center on the value.
   */
  public static HashMap<Integer, DoubleVector> readOutput(Configuration conf,
      Path out, Path centerPath, FileSystem fs) throws IOException {
    HashMap<Integer, DoubleVector> centerMap = new HashMap<Integer, DoubleVector>();
    SequenceFile.Reader centerReader = new SequenceFile.Reader(fs, centerPath,
        conf);
    int index = 0;
    VectorWritable center = new VectorWritable();
    while (centerReader.next(center, NullWritable.get())) {
      centerMap.put(index++, center.getVector());
    }
    centerReader.close();
    return centerMap;
  }

  /**
   * Reads input text files and writes it to a sequencefile.
   */
  public static Path prepareInputText(int k, Configuration conf, Path txtIn,
      Path center, Path out, FileSystem fs) throws IOException {

    Path in;
    if (fs.isFile(txtIn)) {
      in = new Path(txtIn.getParent(), "textinput/in.seq");
    } else {
      in = new Path(txtIn, "textinput/in.seq");
    }

    if (fs.exists(out))
      fs.delete(out, true);

    if (fs.exists(center))
      fs.delete(center, true);

    if (fs.exists(in))
      fs.delete(in, true);

    final NullWritable value = NullWritable.get();

    Writer centerWriter = new SequenceFile.Writer(fs, conf, center,
        VectorWritable.class, NullWritable.class);

    final SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf,
        in, VectorWritable.class, NullWritable.class, CompressionType.NONE);

    int i = 0;

    BufferedReader br = new BufferedReader(
        new InputStreamReader(fs.open(txtIn)));
    String line;
    while ((line = br.readLine()) != null) {
      String[] split = line.split("\t");
      DenseDoubleVector vec = new DenseDoubleVector(split.length);
      for (int j = 0; j < split.length; j++) {
        vec.set(j, Double.parseDouble(split[j]));
      }
      VectorWritable vector = new VectorWritable(vec);
      dataWriter.append(vector, value);
      if (k > i) {
          assert centerWriter != null;
          centerWriter.append(vector, value);
      } else {
        if (centerWriter != null) {
          centerWriter.close();
          centerWriter = null;
        }
      }
      i++;
    }
    br.close();
    dataWriter.close();
    return in;
  }

  /**
   * Create some random vectors as input and assign the first k vectors as
   * intial centers.
   */
  public static void prepareInput(int count, int k, int dimension,
      Configuration conf, Path in, Path center, Path out, FileSystem fs)
      throws IOException {
    if (fs.exists(out))
      fs.delete(out, true);

    if (fs.exists(center))
      fs.delete(out, true);

    if (fs.exists(in))
      fs.delete(in, true);

    final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs,
        conf, center, VectorWritable.class, NullWritable.class,
        CompressionType.NONE);
    final NullWritable value = NullWritable.get();

    final SequenceFile.Writer dataWriter = SequenceFile.createWriter(fs, conf,
        in, VectorWritable.class, NullWritable.class, CompressionType.NONE);

    Random r = new Random();
    for (int i = 0; i < count; i++) {

      double[] arr = new double[dimension];
      for (int d = 0; d < dimension; d++) {
        arr[d] = r.nextInt(count);
      }
      VectorWritable vector = new VectorWritable(new DenseDoubleVector(arr));
      dataWriter.append(vector, value);
      if (k > i) {
        centerWriter.append(vector, value);
      } else if (k == i) {
        centerWriter.close();
      }
    }
    dataWriter.close();
  }
}
