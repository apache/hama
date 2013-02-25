package org.apache.hama.graph;

import java.util.List;
import java.util.PriorityQueue;

import junit.framework.TestCase;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestGraphJobMessage extends TestCase {

  @Test
  public void testPriorityQueue() {
    PriorityQueue<GraphJobMessage> prio = new PriorityQueue<GraphJobMessage>();
    prio.addAll(getMessages());

    GraphJobMessage poll = prio.poll();
    assertEquals(true, poll.isMapMessage());
    poll = prio.poll();
    assertEquals(true, poll.isVertexMessage());
    assertEquals("1", poll.getVertexId().toString());

    poll = prio.poll();
    assertEquals(true, poll.isVertexMessage());
    assertEquals("2", poll.getVertexId().toString());

    poll = prio.poll();
    assertEquals(true, poll.isVertexMessage());
    assertEquals("3", poll.getVertexId().toString());

    assertTrue(prio.isEmpty());
  }

  public List<GraphJobMessage> getMessages() {
    GraphJobMessage mapMsg = new GraphJobMessage(new MapWritable());
    GraphJobMessage vertexMsg1 = new GraphJobMessage(new Text("1"),
        new IntWritable());
    GraphJobMessage vertexMsg2 = new GraphJobMessage(new Text("2"),
        new IntWritable());
    GraphJobMessage vertexMsg3 = new GraphJobMessage(new Text("3"),
        new IntWritable());
    return Lists.newArrayList(mapMsg, vertexMsg1, vertexMsg2, vertexMsg3);
  }

}
