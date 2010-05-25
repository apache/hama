package org.apache.hama.bsp;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class UserInterface extends HamaCluster implements Watcher {
  private HamaConfiguration conf;
  private String JOBNAME = "hama.test.bsp";
  private Path INPUTPATH = new Path("/tmp/input");
  private Path OUTPUTPATH = new Path("/tmp/output");
  
  public UserInterface() {
    this.conf = getConf();
  }

  public void testScenario() throws InterruptedException, IOException {
    // BSP job configuration
    BSPJob bsp = new BSPJob(this.conf);
    // Set the job name
    bsp.setJobName(JOBNAME);

    // Set in/output path and formatter
//    bsp.setInputPath(conf, INPUTPATH);
//    bsp.setOutputPath(conf, OUTPUTPATH);
    bsp.setInputFormat(MyInputFormat.class);
    bsp.setOutputFormat(MyOutputFormat.class);

    // Set the BSP code
    bsp.setBSPCode(MyBSP.class);
    bsp.submit();

    //*******************
    // assertion checking
    assertEquals(bsp.getJobName(), JOBNAME);
    //assertEquals(bsp.getInputPath(), INPUTPATH);
    //assertEquals(bsp.getOutputPath(), OUTPUTPATH);
  }

  class MyBSP implements BSPInterface {
    // TODO: implement some BSP example
  }

  class MyInputFormat extends InputFormat {

    @Override
    public RecordReader createRecordReader(InputSplit arg0,
        TaskAttemptContext arg1) throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public List getSplits(JobContext arg0) throws IOException,
        InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }
    // TODO: implement some input Formatter
  }
  
  class MyOutputFormat extends OutputFormat {

    @Override
    public void checkOutputSpecs(JobContext arg0) throws IOException,
        InterruptedException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext arg0)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext arg0)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }
    // TODO: implement some input Formatter
  }

  @Override
  public void process(WatchedEvent event) {
    // TODO Auto-generated method stub
    
  }
}
