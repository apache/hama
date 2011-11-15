package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * A message that consists of a string tag and a long data. 
 */
public class LongMessage extends BSPMessage {

  private String tag;
  private long data;

  public LongMessage() {
    super();
  }

  public LongMessage(String tag, long data) {
    super();
    this.data = data;
    this.tag = tag;
  }

  @Override
  public String getTag() {
    return tag;
  }

  @Override
  public Long getData() {
    return data;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(tag);
    out.writeLong(data);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tag = in.readUTF();
    data = in.readLong();
  }

}
