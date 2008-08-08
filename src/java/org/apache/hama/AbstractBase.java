package org.apache.hama;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class AbstractBase {

  public int bytesToInt(byte[] b) {
    return Integer.parseInt(Bytes.toString(b));
  }
  
  public byte[] intToBytes(int d) {
    return Bytes.toBytes(String.valueOf(d));
  }
  
  public double bytesToDouble(byte[] b) {
    return Double.parseDouble(Bytes.toString(b));
  }

  public byte[] doubleToBytes(Double d) {
    return Bytes.toBytes(d.toString());
  }

  public int getColumnIndex(byte[] b) {
    String cKey = new String(b);
    return Integer.parseInt(cKey
        .substring(cKey.indexOf(":") + 1, cKey.length()));
  }
}
