package org.apache.hama.examples;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.graph.VertexArrayWritable;
import org.apache.hama.graph.VertexWritable;

/**
 * Testcase for {@link ShortestPaths}
 */

public class SSSPTest extends TestCase {

  private static final Map<VertexWritable, VertexArrayWritable> testData = new HashMap<VertexWritable, VertexArrayWritable>();

  static {
    String[] cities = new String[] { "Frankfurt", "Mannheim", "Wuerzburg",
        "Stuttgart", "Kassel", "Karlsruhe", "Erfurt", "Nuernberg", "Augsburg",
        "Muenchen" };

    for (String city : cities) {
      if (city.equals("Frankfurt")) {
        VertexWritable[] textArr = new VertexWritable[3];
        textArr[0] = new VertexWritable(85, "Mannheim");
        textArr[1] = new VertexWritable(173, "Kassel");
        textArr[2] = new VertexWritable(217, "Wuerzburg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Stuttgart")) {
        VertexWritable[] textArr = new VertexWritable[1];
        textArr[0] = new VertexWritable(183, "Nuernberg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Kassel")) {
        VertexWritable[] textArr = new VertexWritable[2];
        textArr[0] = new VertexWritable(502, "Muenchen");
        textArr[1] = new VertexWritable(173, "Frankfurt");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Erfurt")) {
        VertexWritable[] textArr = new VertexWritable[1];
        textArr[0] = new VertexWritable(186, "Wuerzburg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Wuerzburg")) {
        VertexWritable[] textArr = new VertexWritable[3];
        textArr[0] = new VertexWritable(217, "Frankfurt");
        textArr[1] = new VertexWritable(186, "Erfurt");
        textArr[2] = new VertexWritable(103, "Nuernberg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Mannheim")) {
        VertexWritable[] textArr = new VertexWritable[2];
        textArr[0] = new VertexWritable(80, "Karlsruhe");
        textArr[1] = new VertexWritable(85, "Frankfurt");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Karlsruhe")) {
        VertexWritable[] textArr = new VertexWritable[2];
        textArr[0] = new VertexWritable(250, "Augsburg");
        textArr[1] = new VertexWritable(80, "Mannheim");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Augsburg")) {
        VertexWritable[] textArr = new VertexWritable[2];
        textArr[0] = new VertexWritable(250, "Karlsruhe");
        textArr[1] = new VertexWritable(84, "Muenchen");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Nuernberg")) {
        VertexWritable[] textArr = new VertexWritable[3];
        textArr[0] = new VertexWritable(183, "Stuttgart");
        textArr[1] = new VertexWritable(167, "Muenchen");
        textArr[2] = new VertexWritable(103, "Wuerzburg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      } else if (city.equals("Muenchen")) {
        VertexWritable[] textArr = new VertexWritable[3];
        textArr[0] = new VertexWritable(167, "Nuernberg");
        textArr[1] = new VertexWritable(502, "Kassel");
        textArr[2] = new VertexWritable(84, "Augsburg");
        VertexArrayWritable arr = new VertexArrayWritable();
        arr.set(textArr);
        testData.put(new VertexWritable(0, city), arr);
      }
    }
  }

  private static String INPUT = "/tmp/sssp-tmp.seq";
  private static String TEXT_INPUT = "/tmp/sssp.txt";
  private static String TEXT_OUTPUT = INPUT + "sssp.txt.seq";
  private static String OUTPUT = "/tmp/sssp-out";
  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  public void testShortestPaths() throws IOException, InterruptedException,
      ClassNotFoundException, InstantiationException, IllegalAccessException {

    generateTestSequenceFileData();
    try {
      SSSP.main(new String[] { "Frankfurt", INPUT, OUTPUT });

      //FIXME verifyResult();
    } finally {
      deleteTempDirs();
    }
  }

  private void verifyResult() throws IOException {
    Map<String, Integer> rs = new HashMap<String, Integer>();
    rs.put("Erfurt", 403);
    rs.put("Mannheim", 85);
    rs.put("Stuttgart", 503);
    rs.put("Kassel", 173);
    rs.put("Nuernberg", 320);
    rs.put("Augsburg", 415);
    rs.put("Frankfurt", 0);
    rs.put("Muenchen", 487);
    rs.put("Wuerzburg", 217);
    rs.put("Karlsruhe", 165);

    SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(OUTPUT
        + "/part-00000"), conf);
    Text key = new Text();
    IntWritable value = new IntWritable();
    while (reader.next(key, value)) {
      assertEquals(value.get(), (int) rs.get(key.toString()));
    }
  }

  private void generateTestSequenceFileData() throws IOException {
    SequenceFile.Writer writer = SequenceFile
        .createWriter(fs, conf, new Path(INPUT), VertexWritable.class,
            VertexArrayWritable.class);
    for (Map.Entry<VertexWritable, VertexArrayWritable> e : testData
        .entrySet()) {
      writer.append(e.getKey(), e.getValue());
    }
    writer.close();
  }

  private void generateTestTextData() throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(TEXT_INPUT));
    for (Map.Entry<VertexWritable, VertexArrayWritable> e : testData
        .entrySet()) {
      writer.write(e.getKey().getName() + "\t");
      for (int i = 0; i < e.getValue().get().length; i++) {
        writer.write(((VertexWritable) e.getValue().get()[i]).getName()
            + ":" + ((VertexWritable) e.getValue().get()[i]).getWeight()
            + "\t");
      }
      writer.write("\n");
    }
    writer.close();
  }

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(INPUT)))
        fs.delete(new Path(INPUT), true);
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
      if (fs.exists(new Path(TEXT_INPUT)))
        fs.delete(new Path(TEXT_INPUT), true);
      if (fs.exists(new Path(TEXT_OUTPUT)))
        fs.delete(new Path(TEXT_OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
