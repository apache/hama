package org.apache.hama.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.Combiner;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.util.KeyValuePair;

public abstract class GraphJobRunnerBase<V extends Writable, E extends Writable, M extends Writable>
    extends BSP<Writable, Writable, Writable, Writable, GraphJobMessage> {

  static final Log LOG = LogFactory.getLog(GraphJobRunner.class);

  // make sure that these values don't collide with the vertex names
  protected static final String S_FLAG_MESSAGE_COUNTS = "hama.0";
  protected static final String S_FLAG_AGGREGATOR_VALUE = "hama.1";
  protected static final String S_FLAG_AGGREGATOR_INCREMENT = "hama.2";

  protected static final Text FLAG_MESSAGE_COUNTS = new Text(
      S_FLAG_MESSAGE_COUNTS);

  public static final String MESSAGE_COMBINER_CLASS = "hama.vertex.message.combiner.class";
  public static final String GRAPH_REPAIR = "hama.graph.repair";

  protected Configuration conf;
  protected Combiner<M> combiner;
  protected Partitioner<V, M> partitioner;

  // multiple aggregator arrays
  protected Aggregator<M, Vertex<V, E, M>>[] aggregators;
  protected Writable[] globalAggregatorResult;
  protected IntWritable[] globalAggregatorIncrement;
  protected boolean[] isAbstractAggregator;
  protected String[] aggregatorClassNames;
  protected Text[] aggregatorValueFlag;
  protected Text[] aggregatorIncrementFlag;
  // aggregator on the master side
  protected Aggregator<M, Vertex<V, E, M>>[] masterAggregator;

  protected Map<V, Vertex<V, E, M>> vertices = new HashMap<V, Vertex<V, E, M>>();

  protected String masterTask;
  protected boolean updated = true;
  protected int globalUpdateCounts = 0;

  protected long numberVertices = 0;
  // -1 is deactivated
  protected int maxIteration = -1;
  protected long iteration;

  protected Class<V> vertexIdClass;
  protected Class<M> vertexValueClass;
  protected Class<E> edgeValueClass;
  protected Class<Vertex<V, E, M>> vertexClass;

  @SuppressWarnings("unchecked")
  protected void loadVertices(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
      boolean repairNeeded, boolean runtimePartitioning,
      Partitioner<V, M> partitioner,
      VertexInputReader<Writable, Writable, V, E, M> reader,
      GraphJobRunner<V, E, M> graphJobRunner) throws IOException,
      SyncException, InterruptedException {

    // //////////////////////////////////
    long splitSize = peer.getSplitSize();
    int partitioningSteps = computeMultiSteps(peer, splitSize);
    long interval = splitSize / partitioningSteps;
    // //////////////////////////////////

    LOG.debug("vertex class: " + vertexClass);
    boolean selfReference = conf.getBoolean("hama.graph.self.ref", false);
    Vertex<V, E, M> vertex = newVertexInstance(vertexClass, conf);
    vertex.setPeer(peer);
    vertex.runner = graphJobRunner;

    long startPos = peer.getPos();
    if (startPos == 0)
      startPos = 1L;

    KeyValuePair<Writable, Writable> next = null;
    int steps = 1;
    while ((next = peer.readNext()) != null) {
      boolean vertexFinished = reader.parseVertex(next.getKey(),
          next.getValue(), vertex);
      if (!vertexFinished) {
        continue;
      }

      if (vertex.getEdges() == null) {
        vertex.setEdges(new ArrayList<Edge<V, E>>(0));
      }
      if (selfReference) {
        vertex.addEdge(new Edge<V, E>(vertex.getVertexID(), peer.getPeerName(),
            null));
      }
      if (runtimePartitioning) {
        int partition = partitioner.getPartition(vertex.getVertexID(),
            vertex.getValue(), peer.getNumPeers());
        // set the destination name for the edge now
        for (Edge<V, E> edge : vertex.getEdges()) {
          int edgePartition = partitioner.getPartition(
              edge.getDestinationVertexID(), (M) edge.getValue(),
              peer.getNumPeers());
          edge.destinationPeerName = peer.getPeerName(edgePartition);
        }
        peer.send(peer.getPeerName(partition), new GraphJobMessage(vertex));
      } else {
        // FIXME need to set destination names
        vertex.setup(conf);
        vertices.put(vertex.getVertexID(), vertex);
      }
      vertex = newVertexInstance(vertexClass, conf);
      vertex.setPeer(peer);
      vertex.runner = graphJobRunner;

      if (runtimePartitioning) {
        if (steps < partitioningSteps && (peer.getPos() - startPos) >= interval) {
          peer.sync();
          steps++;
          GraphJobMessage msg = null;
          while ((msg = peer.getCurrentMessage()) != null) {
            Vertex<V, E, M> messagedVertex = (Vertex<V, E, M>) msg.getVertex();
            messagedVertex.setPeer(peer);
            messagedVertex.runner = graphJobRunner;
            messagedVertex.setup(conf);
            vertices.put(messagedVertex.getVertexID(), messagedVertex);
          }
          startPos = peer.getPos();
        }
      }
    }

    if (runtimePartitioning) {
      peer.sync();

      GraphJobMessage msg = null;
      while ((msg = peer.getCurrentMessage()) != null) {
        Vertex<V, E, M> messagedVertex = (Vertex<V, E, M>) msg.getVertex();
        messagedVertex.setPeer(peer);
        messagedVertex.runner = graphJobRunner;
        messagedVertex.setup(conf);
        vertices.put(messagedVertex.getVertexID(), messagedVertex);
      }
    }
    LOG.debug("Loading finished at " + peer.getSuperstepCount() + " steps.");

    /*
     * If the user want to repair the graph, it should traverse through that
     * local chunk of adjancency list and message the corresponding peer to
     * check whether that vertex exists. In real-life this may be dead-ending
     * vertices, since we have no information about outgoing edges. Mainly this
     * procedure is to prevent NullPointerExceptions from happening.
     */
    if (repairNeeded) {
      LOG.debug("Starting repair of this graph!");

      int multiSteps = 0;
      MapWritable ssize = new MapWritable();
      ssize.put(new IntWritable(peer.getPeerIndex()),
          new IntWritable(vertices.size()));
      peer.send(masterTask, new GraphJobMessage(ssize));
      ssize = null;
      peer.sync();

      if (this.isMasterTask(peer)) {
        int minVerticesSize = Integer.MAX_VALUE;
        GraphJobMessage received = null;
        while ((received = peer.getCurrentMessage()) != null) {
          MapWritable x = received.getMap();
          for (Entry<Writable, Writable> e : x.entrySet()) {
            int curr = ((IntWritable) e.getValue()).get();
            if (minVerticesSize > curr) {
              minVerticesSize = curr;
            }
          }
        }

        if (minVerticesSize < (partitioningSteps * 2)) {
          multiSteps = minVerticesSize;
        } else {
          multiSteps = (partitioningSteps * 2);
        }

        for (String peerName : peer.getAllPeerNames()) {
          MapWritable temp = new MapWritable();
          temp.put(new Text("steps"), new IntWritable(multiSteps));
          peer.send(peerName, new GraphJobMessage(temp));
        }
      }
      peer.sync();

      GraphJobMessage received = peer.getCurrentMessage();
      MapWritable x = received.getMap();
      for (Entry<Writable, Writable> e : x.entrySet()) {
        multiSteps = ((IntWritable) e.getValue()).get();
      }

      Set<V> keys = vertices.keySet();
      Map<V, Vertex<V, E, M>> tmp = new HashMap<V, Vertex<V, E, M>>();

      int i = 0;
      int syncs = 0;
      for (V v : keys) {
        for (Edge<V, E> e : vertices.get(v).getEdges()) {
          peer.send(e.getDestinationPeerName(),
              new GraphJobMessage(e.getDestinationVertexID()));
        }

        if (syncs < multiSteps && (i % (vertices.size() / multiSteps)) == 0) {
          peer.sync();
          syncs++;
          GraphJobMessage msg = null;
          while ((msg = peer.getCurrentMessage()) != null) {
            V vertexName = (V) msg.getVertexId();
            if (!vertices.containsKey(vertexName)) {
              Vertex<V, E, M> newVertex = newVertexInstance(vertexClass, conf);
              newVertex.setPeer(peer);
              newVertex.setVertexID(vertexName);
              newVertex.runner = graphJobRunner;
              if (selfReference) {
                int partition = partitioner.getPartition(
                    newVertex.getVertexID(), newVertex.getValue(),
                    peer.getNumPeers());
                String target = peer.getPeerName(partition);
                newVertex.setEdges(Collections.singletonList(new Edge<V, E>(
                    newVertex.getVertexID(), target, null)));
              } else {
                newVertex.setEdges(new ArrayList<Edge<V, E>>(0));
              }
              newVertex.setup(conf);
              tmp.put(vertexName, newVertex);
            }
          }
        }
        i++;
      }

      peer.sync();
      GraphJobMessage msg = null;
      while ((msg = peer.getCurrentMessage()) != null) {
        V vertexName = (V) msg.getVertexId();
        if (!vertices.containsKey(vertexName)) {
          Vertex<V, E, M> newVertex = newVertexInstance(vertexClass, conf);
          newVertex.setPeer(peer);
          newVertex.setVertexID(vertexName);
          newVertex.runner = graphJobRunner;
          if (selfReference) {
            int partition = partitioner.getPartition(newVertex.getVertexID(),
                newVertex.getValue(), peer.getNumPeers());
            String target = peer.getPeerName(partition);
            newVertex.setEdges(Collections.singletonList(new Edge<V, E>(
                newVertex.getVertexID(), target, null)));
          } else {
            newVertex.setEdges(new ArrayList<Edge<V, E>>(0));
          }
          newVertex.setup(conf);
          vertices.put(vertexName, newVertex);
          newVertex = null;
        }
      }

      for (Map.Entry<V, Vertex<V, E, M>> e : tmp.entrySet()) {
        vertices.put(e.getKey(), e.getValue());
      }
      tmp.clear();
    }

    LOG.debug("Starting Vertex processing!");
  }

  protected int computeMultiSteps(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
      long splitSize) throws IOException, SyncException, InterruptedException {
    int multiSteps = 1;

    MapWritable ssize = new MapWritable();
    ssize
        .put(new IntWritable(peer.getPeerIndex()), new LongWritable(splitSize));
    peer.send(masterTask, new GraphJobMessage(ssize));
    ssize = null;
    peer.sync();

    if (this.isMasterTask(peer)) {
      long maxSplitSize = 0L;
      GraphJobMessage received = null;
      while ((received = peer.getCurrentMessage()) != null) {
        MapWritable x = received.getMap();
        for (Entry<Writable, Writable> e : x.entrySet()) {
          long curr = ((LongWritable) e.getValue()).get();
          if (maxSplitSize < curr) {
            maxSplitSize = curr;
          }
        }
      }

      int steps = (int) (maxSplitSize / conf.getInt( // 20 mb
          "hama.graph.multi.step.partitioning.interval", 20000000)) + 1;

      for (String peerName : peer.getAllPeerNames()) {
        MapWritable temp = new MapWritable();
        temp.put(new Text("max"), new IntWritable(steps));
        peer.send(peerName, new GraphJobMessage(temp));
      }
    }
    peer.sync();

    GraphJobMessage received = peer.getCurrentMessage();
    MapWritable x = received.getMap();
    for (Entry<Writable, Writable> e : x.entrySet()) {
      multiSteps = ((IntWritable) e.getValue()).get();
    }
    LOG.info(peer.getPeerName() + ": " + multiSteps);
    return multiSteps;
  }

  /**
   * @return a new vertex instance
   */
  public static <V extends Writable, E extends Writable, M extends Writable> Vertex<V, E, M> newVertexInstance(
      Class<?> vertexClass, Configuration conf) {
    @SuppressWarnings("unchecked")
    Vertex<V, E, M> vertex = (Vertex<V, E, M>) ReflectionUtils.newInstance(
        vertexClass, conf);
    return vertex;
  }

  protected void runAggregators(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer,
      int activeVertices) throws IOException {
    // send msgCounts to the master task
    MapWritable updatedCnt = new MapWritable();
    updatedCnt.put(FLAG_MESSAGE_COUNTS, new IntWritable(activeVertices));
    // also send aggregated values to the master
    if (aggregators != null) {
      for (int i = 0; i < this.aggregators.length; i++) {
        updatedCnt.put(aggregatorValueFlag[i], aggregators[i].getValue());
        if (isAbstractAggregator[i]) {
          updatedCnt.put(aggregatorIncrementFlag[i],
              ((AbstractAggregator<M, Vertex<V, E, M>>) aggregators[i])
                  .getTimesAggregated());
        }
      }
      for (int i = 0; i < aggregators.length; i++) {
        // now create new aggregators for the next iteration
        aggregators[i] = getNewAggregator(aggregatorClassNames[i]);
        if (isMasterTask(peer)) {
          masterAggregator[i] = getNewAggregator(aggregatorClassNames[i]);
        }
      }
    }
    peer.send(masterTask, new GraphJobMessage(updatedCnt));
  }

  @SuppressWarnings("unchecked")
  protected Map<V, LinkedList<M>> parseMessages(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer)
      throws IOException {
    GraphJobMessage msg = null;
    final Map<V, LinkedList<M>> msgMap = new HashMap<V, LinkedList<M>>();
    while ((msg = peer.getCurrentMessage()) != null) {
      // either this is a vertex message or a directive that must be read
      // as map
      if (msg.isVertexMessage()) {
        final V vertexID = (V) msg.getVertexId();
        final M value = (M) msg.getVertexValue();
        LinkedList<M> msgs = msgMap.get(vertexID);
        if (msgs == null) {
          msgs = new LinkedList<M>();
          msgMap.put(vertexID, msgs);
        }
        msgs.add(value);
      } else if (msg.isMapMessage()) {
        for (Entry<Writable, Writable> e : msg.getMap().entrySet()) {
          Text vertexID = (Text) e.getKey();
          if (FLAG_MESSAGE_COUNTS.equals(vertexID)) {
            if (((IntWritable) e.getValue()).get() == Integer.MIN_VALUE) {
              updated = false;
            } else {
              globalUpdateCounts += ((IntWritable) e.getValue()).get();
            }
          } else if (aggregators != null
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_VALUE)) {
            int index = Integer.parseInt(vertexID.toString().split(";")[1]);
            masterAggregator[index].aggregate(null, (M) e.getValue());
          } else if (aggregators != null
              && vertexID.toString().startsWith(S_FLAG_AGGREGATOR_INCREMENT)) {
            int index = Integer.parseInt(vertexID.toString().split(";")[1]);
            if (isAbstractAggregator[index]) {
              ((AbstractAggregator<M, Vertex<V, E, M>>) masterAggregator[index])
                  .addTimesAggregated(((IntWritable) e.getValue()).get());
            }
          }
        }
      } else {
        throw new UnsupportedOperationException("Unknown message type? " + msg);
      }

    }
    return msgMap;
  }

  @SuppressWarnings("unchecked")
  protected Aggregator<M, Vertex<V, E, M>> getNewAggregator(String clsName) {
    try {
      return (Aggregator<M, Vertex<V, E, M>>) ReflectionUtils.newInstance(
          conf.getClassByName(clsName), conf);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    throw new IllegalArgumentException("Aggregator class " + clsName
        + " could not be found or instantiated!");
  }

  protected boolean isMasterTask(
      BSPPeer<Writable, Writable, Writable, Writable, GraphJobMessage> peer) {
    return peer.getPeerName().equals(masterTask);
  }

  public final long getNumberVertices() {
    return numberVertices;
  }

  public final long getNumberIterations() {
    return iteration;
  }

  public final int getMaxIteration() {
    return maxIteration;
  }

  public Partitioner<V, M> getPartitioner() {
    return partitioner;
  }

  public final Writable getLastAggregatedValue(int index) {
    return globalAggregatorResult[index];
  }

  public final IntWritable getNumLastAggregatedVertices(int index) {
    return globalAggregatorIncrement[index];
  }

}
