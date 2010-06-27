package org.apache.hama.zookeeper;

import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hama.HamaConfiguration;

public class ZKServerTool {
  /**
   * Run the tool.
   * 
   * @param args Command line arguments. First arg is path to zookeepers file.
   */
  public static void main(String args[]) {
    Configuration conf = new HamaConfiguration();
    // Note that we do not simply grab the property ZOOKEEPER_QUORUM from
    // the HamaConfiguration because the user may be using a zoo.cfg file.
    Properties zkProps = QuorumPeer.makeZKProps(conf);
    for (Entry<Object, Object> entry : zkProps.entrySet()) {
      String key = entry.getKey().toString().trim();
      String value = entry.getValue().toString().trim();
      if (key.startsWith("server.")) {
        String[] parts = value.split(":");
        String host = parts[0];
        System.out.println(host);
      }
    }
  }
}
