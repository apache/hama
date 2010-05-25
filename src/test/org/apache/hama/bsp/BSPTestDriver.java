package org.apache.hama.bsp;

import java.io.IOException;

import org.apache.hama.HamaConfiguration;

public class BSPTestDriver {

  /**
   * @param args
   * @throws IOException 
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    BSPJob job = new BSPJob(new HamaConfiguration());
    job.setJarByClass(Work.class);
    job.setWorkClass(Work.class);
    job.submit();
    Thread.sleep(3000);

    System.out.println("job id:"+job.getJobID());
    System.out.println("job Name:"+job.getJobName());
    System.out.println("working dir:"+job.getWorkingDirectory());
  }
}
