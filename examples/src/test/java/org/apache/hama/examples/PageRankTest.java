package org.apache.hama.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;

public class PageRankTest extends TestCase {

  private static String INPUT = "/tmp/pagerank-tmp.seq";
  private static String OUTPUT = "/tmp/pagerank-out";
  private Configuration conf;
  private FileSystem fs;

  public void testPageRank() throws IOException, InterruptedException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {
    conf = new HamaConfiguration();
    fs = FileSystem.get(conf);

    generateTestData();
    try {
      PageRank.main(new String[] { INPUT, OUTPUT, "0.85", "0.000001" });
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void verifyResult() throws IOException {
    Map<String, Double> rs = new HashMap<String, Double>();
    // our desired results
    rs.put("stackoverflow.com", 0.20495476070571675);
    rs.put("google.com", 0.339831187357033);
    rs.put("facebook.com", 0.042503114866791786);
    rs.put("yahoo.com", 0.2134265215074906);
    rs.put("twitter.com", 0.042503114866791786);
    rs.put("nasa.gov", 0.12688096846583075);
    rs.put("youtube.com", 0.029900332230345304);

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT
        + "/part-00000"), conf);
    Text key = new Text();
    DoubleWritable value = new DoubleWritable();
    double sum = 0.0;
    while (reader.next(key, value)) {
      double result = (double) rs.get(key.toString());
      assertEquals(value.get(), result);
      sum += value.get();
    }
    System.out.println("Sum is: " + sum);
    assertEquals(sum, 1.0d);
  }

  /**
   * The graph looks like this (adjacency list, [] contains outlinks):<br/>
   * stackoverflow.com [yahoo.com] <br/>
   * google.com []<br/>
   * facebook.com [twitter.com, google.com, nasa.gov]<br/>
   * yahoo.com [nasa.gov, stackoverflow.com]<br/>
   * twitter.com [google.com, facebook.com]<br/>
   * nasa.gov [yahoo.com, stackoverflow.com]<br/>
   * youtube.com [google.com, yahoo.com]<br/>
   */
  private void generateTestData() throws IOException {
    Map<VertexWritable, VertexArrayWritable> tmp = new HashMap<VertexWritable, VertexArrayWritable>();

    // our first entry is null, because our indices in hama 3.0 pre calculated
    // example starts at 1.
    // FIXME This is really ugly.
    String[] pages = new String[] { null, "twitter.com", "google.com",
        "facebook.com", "yahoo.com", "nasa.gov", "stackoverflow.com",
        "youtube.com" };
    String[] lineArray = new String[] { "1;2;3", "2", "3;1;2;5", "4;5;6",
        "5;4;6", "6;4", "7;2;4" };

    for (int i = 0; i < lineArray.length; i++) {

      String[] adjacencyStringArray = lineArray[i].split(";");
      int vertexId = Integer.parseInt(adjacencyStringArray[0]);
      String name = pages[vertexId];
      VertexWritable[] arr = new VertexWritable[adjacencyStringArray.length - 1];
      for (int j = 1; j < adjacencyStringArray.length; j++) {
        arr[j - 1] = new VertexWritable(
            pages[Integer.parseInt(adjacencyStringArray[j])]);
      }
      VertexArrayWritable wr = new VertexArrayWritable();
      wr.set(arr);
      tmp.put(new VertexWritable(name), wr);
    }

    SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, new Path(
        INPUT), VertexWritable.class, VertexArrayWritable.class);
    for (Map.Entry<VertexWritable, VertexArrayWritable> e : tmp.entrySet()) {
      writer.append(e.getKey(), e.getValue());
    }
    writer.close();
  }

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(INPUT)))
        fs.delete(new Path(INPUT), true);
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
