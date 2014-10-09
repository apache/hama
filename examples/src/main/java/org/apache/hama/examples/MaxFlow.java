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
package org.apache.hama.examples;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.commons.io.FloatArrayWritable;
import org.apache.hama.graph.AbstractAggregator;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

/**
 * Maxflow algorithm, find the max flow from source vertex to sink vertex in a
 * graph.
 */

public class MaxFlow {
  public static class MaxFlowVertex extends
      Vertex<Text, FloatArrayWritable, FloatArrayWritable> {
    static double DAMPING_FACTOR = 0.85;
    static double MAXIMUM_CONVERGENCE_ERROR = 0.001;
    static String OUTPUT = null;
    static FloatWritable SENSEMSG = new FloatWritable(-1);
    static FloatWritable PUSHMSG = new FloatWritable(-2);
    static FloatWritable OVERFLOWMSG = new FloatWritable(-7);
    static FloatWritable FIRSTPUSHMSG = new FloatWritable(-8);
    static FloatWritable INEDGE = new FloatWritable(-4);
    static FloatWritable OUTEDGE = new FloatWritable(-5);
    private int senseStepCount = 0;
    private int pushStepCount = 0;
    private FloatWritable overFlow = new FloatWritable(0.0f);
    private FloatWritable preSuperStepOverFlow = new FloatWritable(0.0f);
    FloatArrayWritable msg0 = new FloatArrayWritable();
    FloatWritable[] mArray0 = new FloatWritable[5];
    public ArrayList<FloatArrayWritable> senseMsgList = new ArrayList<FloatArrayWritable>();
    public ArrayList<FloatArrayWritable> pushMsgList = new ArrayList<FloatArrayWritable>();
    public LinkedList<Edge<Text, FloatArrayWritable>> inEdges = new LinkedList<Edge<Text, FloatArrayWritable>>();;
    public LinkedList<Edge<Text, FloatArrayWritable>> outEdges = new LinkedList<Edge<Text, FloatArrayWritable>>();
    public LinkedList<Edge<Text, FloatArrayWritable>> flowedOutEdges = new LinkedList<Edge<Text, FloatArrayWritable>>();
    static public LinkedList<FloatArrayWritable> overFlowMsgList = new LinkedList<FloatArrayWritable>();

    @Override
    public void setup(HamaConfiguration conf) {
    }

    public void addInEdge(Edge<Text, FloatArrayWritable> edge) {
      if (this.inEdges == null)
        this.inEdges = new LinkedList<Edge<Text, FloatArrayWritable>>();
      this.inEdges.add(edge);
    }

    public void addOutEdge(Edge<Text, FloatArrayWritable> edge) {
      if (this.outEdges == null)
        this.outEdges = new LinkedList<Edge<Text, FloatArrayWritable>>();
      this.outEdges.add(edge);
    }

    @Override
    public void compute(Iterable<FloatArrayWritable> messages)
        throws IOException {
      if (getSuperstepCount() == 0) {
        aggregate();
        init();
        return;
      }

      if (!getVertexID().equals(getSinkID())) {
        normalVertexCompute(messages);
      }
      ;

      if (getVertexID().equals(getSinkID())) {
        masterVertexCompute(messages);
      }
    }

    private void init() throws IOException {
      for (Edge<Text, FloatArrayWritable> e : getEdges()) {
        if (e.getDestinationVertexID().equals(getVertexID()))
          continue;
        if (getEdgeCapacity(e).get() < 0) {
          float cap = Math.abs(getEdgeCapacity(e).get());
          setEdgeCapacity(e, new FloatWritable(cap));
          setEdgeType(e.getValue(), INEDGE);
          addInEdge(e);
        } else {
          setEdgeType(e.getValue(), OUTEDGE);
          addOutEdge(e);
        }
      }

      if (getVertexID().equals(getSinkID())) {
        senseToNeighbors(new FloatWritable(Float.MAX_VALUE));
        return;
      }
      if (getVertexID().equals(getSourceID())) {
        overFlow.set(Float.MAX_VALUE);
        return;
      }
      voteToHalt();
    }

    private void normalVertexCompute(Iterable<FloatArrayWritable> messages)
        throws IOException {
      aggregate();
      ArrayList<FloatArrayWritable> pMsgList = new ArrayList<FloatArrayWritable>();
      boolean senseToNeighbor = false;
      for (FloatArrayWritable msg : messages) {
        if (msg == null)
          continue;
        if (getMsgType(msg).equals(SENSEMSG)) {
          if (isFirstSense(msg)) {
            senseToNeighbor = true;
          }
          senseMsgList.add(msg);
        } else if (getMsgType(msg).equals(PUSHMSG)) {
          pMsgList.add(msg);
          pushMsgList.add(msg);
        } else if (getMsgType(msg).equals(FIRSTPUSHMSG)) {
          pushFlowToNeighbors();
        }
      }
      if (senseToNeighbor == true) {
        senseToNeighbors(new FloatWritable(Float.MAX_VALUE));
        if (overFlow.get() > 0) {
          sendOverFlowNodeToMaster();
        }
      }
      if (pMsgList.size() > 0) {
        pushMsgComsume(pMsgList);
      }
      voteToHalt();
    }

    private void masterVertexCompute(Iterable<FloatArrayWritable> messages)
        throws IOException {
      receiveFlowFromNeighbors(messages);
      boolean haveActivingNormalVertex = HaveActivingNormalVertex();
      stepStatusDetecting(haveActivingNormalVertex);
      boolean pushStepCompleted = pushStepCompleted(haveActivingNormalVertex);
      boolean senseStepCompleted = senseStepCompleted(haveActivingNormalVertex);
      if (senseStepCompleted) {
        aggregate();
        for (FloatArrayWritable msg : overFlowMsgList) {
          msg0.set(mArray0);
          setMsgType(msg0, FIRSTPUSHMSG);
          setMsgPushStep(msg0, pushStepCount);
          setMsgVertexID(msg0, textToFloat(getVertexID()));
          setMsgFlow(msg0, 0.0f);
          setEdgeType(msg0, new FloatWritable());
          sendMessage(floatToText(getMsgVertexID(msg)), msg0);
        }
        overFlowMsgList.clear();
      }
      if (pushStepCompleted) {
        aggregate();
        if (algorithmCompleted()) {
          log(1111, "maxFlow is: " + overFlow.get());
          printMaxFlowValue(overFlow.get(), OUTPUT + "/maxflow");
          voteToHalt();
        } else {
          preSuperStepOverFlow.set(overFlow.get());
          senseToNeighbors(new FloatWritable(Float.MAX_VALUE));
        }
      }
      ;
    }

    private void senseToNeighbors(FloatWritable needFlow) throws IOException {

      msg0.set(mArray0);
      setMsgSenseStep(msg0, senseStepCount);
      mArray0[0] = SENSEMSG; // pos 0 store msg type.
      setMsgType(msg0, SENSEMSG);
      setMsgVertexID(msg0, textToFloat(getVertexID()));
      for (Edge<Text, FloatArrayWritable> e : inEdges) {
        setEdgeType(msg0, OUTEDGE);
        float nflow = Math.min(getSpareFlow(e), needFlow.get());
        boolean sendMsg = true;
        if (nflow > 0) {
          setNeedFlow(msg0, nflow);
          for (FloatArrayWritable m : senseMsgList) {
            if (getMsgVertexID(m).equals(e.getDestinationVertexID())) {
              sendMsg = false;
              break;
            }
          }
          if (sendMsg == true)
            sendMessage(e.getDestinationVertexID(), msg0);
        }
      }
      for (Edge<Text, FloatArrayWritable> e : outEdges) {
        setEdgeType(msg0, INEDGE);
        setNeedFlow(msg0, Math.min(getSpareFlow(e), needFlow.get()));
        float nflow = Math.min(getSpareFlow(e), needFlow.get());
        boolean sendMsg = true;
        if (nflow > 0) {
          setNeedFlow(msg0, nflow);
          if (!e.getDestinationVertexID().equals(getSinkID())) {

            for (FloatArrayWritable m : senseMsgList) {
              if (getMsgVertexID(m).equals(e.getDestinationVertexID())) {
                sendMsg = false;
                break;
              }
            }
            if (sendMsg == true)
              sendMessage(e.getDestinationVertexID(), msg0);
          }
        }
      }
    }

    private void sendOverFlowNodeToMaster() throws IOException {
      msg0.set(mArray0);
      setMsgType(msg0, OVERFLOWMSG);
      setMsgVertexID(msg0, textToFloat(getVertexID()));
      setMsgFlow(msg0, 0.0f);
      setEdgeType(msg0, new FloatWritable());
      sendMessage(getSinkID(), msg0);
    }

    private void pushMsgComsume(Iterable<FloatArrayWritable> msgList)
        throws IOException {
      receiveFlowFromNeighbors(msgList);
      pushFlowToNeighbors();
    }

    private void receiveFlowFromNeighbors(Iterable<FloatArrayWritable> msgList) {
      for (FloatArrayWritable msg : msgList) {
        if (msg == null)
          continue;
        if (getMsgType(msg).equals(OVERFLOWMSG)) {
          overFlowMsgList.add(msg);
          continue;
        }
        Edge<Text, FloatArrayWritable> e = getEdgeAccordToMsg(msg);
        FloatWritable oldFlow = getEdgeFlow(e);
        FloatWritable flow = getMsgFlow(msg);

        if (getEdgeType(e.getValue()).equals(INEDGE)) {
          oldFlow.set(oldFlow.get() + flow.get());
        } else if (getEdgeType(e.getValue()).equals(OUTEDGE)) {
          oldFlow.set(oldFlow.get() - flow.get());
        }
        overFlow.set(overFlow.get() + flow.get());
      }
    }

    private void pushFlowToNeighbors() throws IOException {
      ArrayList<FloatArrayWritable> inSenseList = new ArrayList<FloatArrayWritable>();
      ArrayList<FloatArrayWritable> outSenseList = new ArrayList<FloatArrayWritable>();
      Iterator<FloatArrayWritable> iter = senseMsgList.iterator();
      while (iter.hasNext()) {
        FloatArrayWritable msg = (FloatArrayWritable) iter.next();
        if (getMsgSenseStep(msg) < senseStepCount) {
          iter.remove();
          continue;
        }
        boolean rm = false;
        for (FloatArrayWritable m : pushMsgList) {
          if (getMsgVertexID(msg).equals(getMsgVertexID(m))) {
            rm = true;
            break;
          }
        }
        if (rm == true) {
          iter.remove();
        }
      }

      for (FloatArrayWritable msg : senseMsgList) {
        if (getEdgeType(msg).equals(INEDGE))
          inSenseList.add(msg);
        else if (getEdgeType(msg).equals(OUTEDGE))
          outSenseList.add(msg);
      }
      msg0.set(mArray0);
      setMsgType(msg0, PUSHMSG);
      setMsgVertexID(msg0, textToFloat(getVertexID()));
      for (FloatArrayWritable msg : outSenseList) {
        Edge<Text, FloatArrayWritable> e = getEdgeAccordToMsg(msg);
        if (overFlow.get() > 0) {
          FloatWritable oldFlow = getEdgeFlow(e);
          FloatWritable cap = getEdgeCapacity(e);
          float flow = Math.min(overFlow.get(), cap.get() - oldFlow.get());
          if (flow > 0) {
            overFlow.set(overFlow.get() - flow);
            oldFlow.set(oldFlow.get() + flow);
            setEdgeType(msg0, INEDGE);
            setMsgFlow(msg0, flow);
            sendMessage(e, msg0);
          }
        } else {
          break;
        }
      }
      for (FloatArrayWritable msg : inSenseList) {
        Edge<Text, FloatArrayWritable> e = getEdgeAccordToMsg(msg);
        if (overFlow.get() > 0) {
          FloatWritable oldFlow = (FloatWritable) e.getValue().get()[0];
          float flow = Math.min(overFlow.get(), oldFlow.get());
          if (flow > 0) {
            overFlow.set(overFlow.get() - flow);
            oldFlow.set(oldFlow.get() - flow);
            setEdgeType(msg0, OUTEDGE);
            setMsgFlow(msg0, flow);
            sendMessage(e, msg0);
          }
        } else {
          break;
        }
      }
    }

    private boolean algorithmCompleted() {
      if (preSuperStepOverFlow.equals(overFlow) && overFlow.get() > 0)
        return true;
      else
        return false;
    }

    private void stepStatusDetecting(Boolean haveActivingNormalVertex) {
      if (!haveActivingNormalVertex) {
        if (senseStepCount == pushStepCount) {
          senseStepCount++;
        } else if (senseStepCount > pushStepCount) {
          pushStepCount++;
        }
      }
    }

    private boolean pushStepCompleted(boolean haveActivingNormalVertex) {
      if (haveActivingNormalVertex)
        return false;
      if (getSuperstepCount() <= 2)
        return false;
      return senseStepCount > pushStepCount;

    }

    private boolean senseStepCompleted(boolean haveActivingNormalVertex) {
      if (haveActivingNormalVertex)
        return false;
      if (getSuperstepCount() <= 2)
        return false;
      return senseStepCount == pushStepCount;
    }

    private boolean HaveActivingNormalVertex() {
      FloatArrayWritable value = getAggregatedValue(0);
      if (value == null)
        return true;
      FloatWritable temp = (FloatWritable) value.get()[0];
      if (temp.get() < getSuperstepCount() - 1) {
        return false;
      } else if (temp.get() == getSuperstepCount() - 1) {
        return true;
      }
      return true;
    }

    private float getSpareFlow(Edge<Text, FloatArrayWritable> e) {
      FloatWritable type = getEdgeType(e.getValue());
      float cap = getEdgeCapacity(e).get();
      float flow = getEdgeFlow(e).get();
      float ret = 0.0f;
      if (type.equals(INEDGE)) {
        ret = cap - flow;
      } else if (type.equals(OUTEDGE)) {
        ret = flow;
      }
      return ret;
    }

    private void aggregate() throws IOException {
      FloatArrayWritable array = new FloatArrayWritable();
      FloatWritable[] farray = new FloatWritable[1];
      array.set(farray);
      farray[0] = new FloatWritable((float) getSuperstepCount());
      aggregate(0, array);
    }

    private Edge<Text, FloatArrayWritable> getEdgeAccordToMsg(
        FloatArrayWritable msg) {
      Text vertexID = floatToText(getMsgVertexID(msg));
      for (Edge<Text, FloatArrayWritable> e : inEdges) {
        if (vertexID.equals(e.getDestinationVertexID()))
          return e;
      }
      for (Edge<Text, FloatArrayWritable> e : outEdges) {
        if (vertexID.equals(e.getDestinationVertexID()))
          return e;
      }
      return null;
    }

    private boolean isFirstSense(FloatArrayWritable msg) {
      if (getMsgSenseStep(msg) > senseStepCount) {
        senseStepCount = getMsgSenseStep(msg);
        pushMsgList.clear();
        return true;
      } else {
        return false;
      }
    }

    private void printMaxFlowValue(float value, String path) throws IOException {
      File f = new File(path);
      OutputStreamWriter in;
      BufferedWriter bw = null;
      try {
        in = new OutputStreamWriter(new FileOutputStream(f));
        bw = new BufferedWriter(in);
        bw.write(String.valueOf(value));
        bw.close();
      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        // e.printStackTrace();
      } finally {
        if (bw != null)
          bw.close();
      }
    }

    private Text getSinkID() {
      return new Text(String.valueOf(getNumVertices() - 1));
    }

    private Text getSourceID() {
      return new Text(String.valueOf(0));
    }

    public static Text floatToText(FloatWritable value) {
      return new Text(String.valueOf((int) (value.get())));
    }

    public static FloatWritable textToFloat(Text value) {
      return new FloatWritable(Float.parseFloat(value.toString()));
    }

    public void setMsgType(FloatArrayWritable msg, FloatWritable value) {
      msg.get()[0] = value;
    }

    public FloatWritable getMsgType(FloatArrayWritable msg) {
      return (FloatWritable) msg.get()[0];
    }

    public FloatWritable getMsgVertexID(FloatArrayWritable msg) {
      return (FloatWritable) msg.get()[1];
    }

    public void setMsgVertexID(FloatArrayWritable msg, FloatWritable value) {
      msg.get()[1] = value;
    }

    public void setEdgeType(FloatArrayWritable msg, FloatWritable type) {
      msg.get()[2] = type;
    }

    public FloatWritable getEdgeType(FloatArrayWritable msg) {
      return (FloatWritable) msg.get()[2];
    }

    public int getMsgSenseStep(FloatArrayWritable msg) {
      FloatWritable value = (FloatWritable) msg.get()[4];
      return (int) value.get();
    }

    public void setMsgSenseStep(FloatArrayWritable msg, int step) {
      FloatWritable v = new FloatWritable(step);
      msg.get()[4] = v;
    }

    public int getMsgPushStep(FloatArrayWritable msg) {
      return getMsgSenseStep(msg);
    }

    public void setMsgPushStep(FloatArrayWritable msg, int step) {
      setMsgSenseStep(msg, step);
    }

    private void setNeedFlow(FloatArrayWritable msg, float value) {
      FloatWritable v = new FloatWritable(value);
      msg.get()[3] = v;
    }

    private FloatWritable getNeedFlow(FloatArrayWritable msg) {
      return (FloatWritable) msg.get()[3];
    }

    private void setMsgFlow(FloatArrayWritable msg, float value) {
      setNeedFlow(msg, value);
    }

    private FloatWritable getMsgFlow(FloatArrayWritable msg) {
      return getNeedFlow(msg);
    }

    private FloatWritable getEdgeFlow(Edge<Text, FloatArrayWritable> e) {
      return getMsgType(e.getValue());
    }

    private FloatWritable getEdgeCapacity(Edge<Text, FloatArrayWritable> e) {
      return getMsgVertexID(e.getValue());
    }

    private void setEdgeCapacity(Edge<Text, FloatArrayWritable> e,
        FloatWritable value) {
      setMsgVertexID(e.getValue(), value);
    }

    public void log(int level, String s) {
      if (level > 1000)
        System.out.println(s);
    }
  }

  public static class MaxFLowAgrregator extends
      AbstractAggregator<FloatArrayWritable> {

    int maxValue = 0;

    @Override
    public void aggregate(FloatArrayWritable value) {
      FloatWritable v = (FloatWritable) value.get()[0];
      if ((int) v.get() > maxValue)
        maxValue = (int) v.get();
    }

    @Override
    public FloatArrayWritable getValue() {
      FloatArrayWritable value = new FloatArrayWritable();
      FloatWritable[] array = new FloatWritable[1];
      FloatWritable f = new FloatWritable();
      f.set(maxValue);
      array[0] = f;
      value.set(array);
      return value;
    }
  }

  public static class MaxFlowSeqReader
      extends
      VertexInputReader<FloatWritable, FloatArrayWritable, Text, FloatArrayWritable, FloatArrayWritable> {
    @Override
    public boolean parseVertex(FloatWritable key, FloatArrayWritable value,
        Vertex<Text, FloatArrayWritable, FloatArrayWritable> vertex)
        throws Exception {
      vertex.setVertexID(MaxFlowVertex.floatToText(key));
      Writable[] values = value.get();
      FloatWritable v2 = (FloatWritable) values[0];
      FloatWritable capacity = (FloatWritable) values[1];
      FloatArrayWritable cost = new FloatArrayWritable();
      FloatWritable[] costArray = new FloatWritable[3];
      costArray[0] = new FloatWritable(0.0f); // store flow
      costArray[1] = capacity; // store capacity
      if (capacity.get() < 0) {
        costArray[2] = new FloatWritable(-4f);
      } else {
        costArray[2] = new FloatWritable(-5f);
      }
      cost.set(costArray);
      Edge<Text, FloatArrayWritable> e = new Edge<Text, FloatArrayWritable>(
          MaxFlowVertex.floatToText(v2), cost);
      (vertex).addEdge(e);
      return true;
    }
  }

  public static GraphJob createJob(String[] args, HamaConfiguration conf)
      throws IOException {
    GraphJob maxFlow = new GraphJob(conf, PageRank.class);
    maxFlow.setJobName("MaxFlow");
    maxFlow.setMessageClass(FloatArrayWritable.class);
    maxFlow.setEdgeValueClass(FloatArrayWritable.class);
    maxFlow.setVertexClass(MaxFlowVertex.class);
    maxFlow.setInputKeyClass(FloatWritable.class);
    maxFlow.setInputValueClass(FloatArrayWritable.class);
    maxFlow.setAggregatorClass(MaxFLowAgrregator.class);
    maxFlow.setInputPath(new Path(args[0]));
    maxFlow.setOutputPath(new Path(args[1]));
    maxFlow.set("hama.graph.self.ref", "true");

    if (args.length == 3) {
      maxFlow.setNumBspTask(Integer.parseInt(args[2]));
    }
    maxFlow.setNumBspTask(3);
    maxFlow.setMaxIteration(Integer.MAX_VALUE);
    maxFlow.setVertexInputReaderClass(MaxFlowSeqReader.class);

    maxFlow.setVertexIDClass(Text.class);
    maxFlow.setVertexValueClass(FloatArrayWritable.class);
    maxFlow.setEdgeValueClass(FloatArrayWritable.class);

    maxFlow.setInputFormat(SequenceFileInputFormat.class);

    maxFlow.setPartitioner(HashPartitioner.class);
    maxFlow.setOutputFormat(TextOutputFormat.class);
    maxFlow.setOutputKeyClass(DoubleWritable.class);
    maxFlow.setOutputValueClass(FloatArrayWritable.class);
    return maxFlow;
  }

  private static void printUsage() {
    System.out.println("Usage: <input> <output> [tasks]");
    System.exit(-1);
  }

  public static void main(String[] args) throws IOException,
      InterruptedException, ClassNotFoundException {
    args[2] = "3";
    if (args.length < 2) {
      printUsage();
      System.exit(-1);
    }

    HamaConfiguration conf = new HamaConfiguration();
    GraphJob mfmcJob = createJob(args, conf);
    long startTime = System.currentTimeMillis();
    MaxFlowVertex.OUTPUT = args[1];
    if (mfmcJob.waitForCompletion(true)) {
      System.out.println("Job Finished in "
          + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }
  }
}
