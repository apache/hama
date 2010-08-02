package org.apache.hama.bsp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

public class BSPTask extends Task {
  private BSP bsp;
  private Configuration conf;
  
  public BSPTask(String jobId, String jobFile, String taskid, int partition, Configuration conf) {
    this.jobId = jobId;
    this.jobFile = jobFile;
    this.taskId = taskid;
    this.partition = partition;
    this.conf = conf;
  }

  public BSP getBSPClass() {
    bsp = (BSP) ReflectionUtils.newInstance(conf.getClass("bsp.work.class",
        BSP.class), conf);
    
    return bsp;
  }

}
