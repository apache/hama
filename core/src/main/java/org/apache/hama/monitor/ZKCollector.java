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
package org.apache.hama.monitor;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.monitor.Metric;
import org.apache.hama.monitor.MetricsRecord;
import org.apache.hama.monitor.Federator.Collector;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Fire a action to harvest metrics from ZooKeeper.
 */
public final class ZKCollector implements Collector {

  public static final Log LOG = LogFactory.getLog(ZKCollector.class); 

  private AtomicReference<Reference> reference = 
    new AtomicReference<Reference>();

  static final class Reference {
    final ZooKeeper zk;
    final MetricsRecord record; // name for this search 
    final String path; // /path/to/metrics/parent/folder 
    public Reference(ZooKeeper zk, MetricsRecord record, String path) {
      this.zk = zk;
      this.record = record;
      this.path = path;
    }
  } 

  /**
   * ZKCollector havests metrics information from ZooKeeper. 
   * @param zk is the target repository storing metrics.
   * @param nameOfRecord is the name of MetricsRecord.
   * @param descOfRecord is the description of MetricsRecord.
   * @param path points to the <b>parent</b> directory of stored metrics. 
   */
  public ZKCollector(ZooKeeper zk, String nameOfRecord, String descOfRecord, 
      String path) {
    Reference ref = this.reference.get();
    if(null == ref) {
      this.reference.set(new Reference(zk, new MetricsRecord(nameOfRecord, 
      descOfRecord), path));
    }
  }

  @Override
  public Object harvest() throws Exception {
    final String path = this.reference.get().path;
    final ZooKeeper zk = this.reference.get().zk;
    LOG.debug("Searching "+path+" in zookeeper.");
    Stat stat = zk.exists(path, false);
    if(null == stat) return null; // no need to collect data.
    List<String> children = zk.getChildren(path, false);
    if(LOG.isDebugEnabled()) {
      LOG.debug("Leaves size is "+children.size()+" total znodes in list: "+
      children);
    }

    // TODO: metrics record contains multiple metrics (1 to many)
    // data is stored under zk e.g. /path/to/metrics/jvm/...
    // within jvm folder metrics is stored in a form of name, value pair 
    final MetricsRecord record = reference.get().record;
    if(null != children) {
      for(String child: children) {
        LOG.info("metrics -> "+child);
        // <metricsName_d> indicates data type is double  
        String dataType = suffix(child); 
        byte[] dataInBytes = zk.getData(path+"/"+child, false, stat);
        if(LOG.isDebugEnabled()) {
          LOG.debug("Data length (in byte): "+dataInBytes.length); 
        }
        String name = removeSuffix(child);
        DataInputStream input = 
          new DataInputStream(new ByteArrayInputStream(dataInBytes));
        if("d".equals(dataType)) {
          double dv = input.readDouble();
          LOG.info("metrics "+name+" value:"+dv);
          record.add(new Metric(name, dv)); 
        } else if("f".equals(dataType)) {
          float fv = input.readFloat();
          LOG.info("metrics "+name+" value:"+fv);
          record.add(new Metric(name, fv)); 
        } else if("i".equals(dataType)) {
          int iv = input.readInt();
          LOG.info("metrics "+name+" value:"+iv);
          record.add(new Metric(name, iv)); 
        } else if("l".equals(dataType)) {
          long lv = input.readLong();
          LOG.info("metrics "+name+" value:"+lv);
          record.add(new Metric(name, lv)); 
        } else if("b".equals(dataType)) {
          LOG.info("metrics"+name+" value:"+dataInBytes);
          record.add(new Metric(name, dataInBytes)); 
        } else {
          LOG.warn("Unkown data type for metrics name: "+child);
        }
        try {} finally{ input.close();}
      }
    }
    return record; 
  }

  private String removeSuffix(String path) {
    return path.substring(0, path.length()-2);
  }

  private String suffix(String path) {
    if(!"_".equals(path.substring(path.length()-2, path.length()-1))) {
      return "?";
    }
    return path.substring(path.length()-1, path.length());
  }   

}
