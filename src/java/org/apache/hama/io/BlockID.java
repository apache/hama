/**
 * Copyright 2007 The Apache Software Foundation
 *
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
package org.apache.hama.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/** A WritableComparable for BlockIDs. */
@SuppressWarnings("unchecked")
public class BlockID implements WritableComparable {
  static final Logger LOG = Logger.getLogger(BlockID.class);
  public static final int PAD_SIZE = 15;
  private int row;
  private int column;

  public BlockID() {
  }

  public BlockID(int row, int column) {
    set(row, column);
  }

  public BlockID(byte[] bytes) throws IOException {
    String rKey = Bytes.toString(bytes);
    String keys[] = null;
    if (rKey.substring(0, 8).equals("00000000")) {
      int i = 8;
      while (rKey.charAt(i) == '0') {
        i++;
      }
      keys = rKey.substring(i, rKey.length()).split("[,]");
    } else {
      int i = 0;
      while (rKey.charAt(i) == '0') {
        i++;
      }
      keys = rKey.substring(i, rKey.length()).split("[,]");
    }

    try {
      this.row = Integer.parseInt(keys[1]);
      this.column = Integer.parseInt(keys[2]);
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new ArrayIndexOutOfBoundsException(rKey + "\n" + e);
    }
  }

  public void set(int row, int column) {
    this.row = row;
    this.column = column;
  }

  public int getRow() {
    return row;
  }

  public int getColumn() {
    return column;
  }

  public void readFields(DataInput in) throws IOException {
    BlockID value = new BlockID(Bytes.readByteArray(in));
    this.row = value.getRow();
    this.column = value.getColumn();
  }

  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, Bytes.toBytes(this.toString()));
  }

  /**
   * Make BlockID's string representation be same format.
   */
  public String toString() {
    int zeros = PAD_SIZE - String.valueOf(row).length()
        - String.valueOf(column).length();
    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < zeros; ++i) {
      buf.append("0");
    }

    return buf.toString() + "," + row + "," + column;
  }

  @Override
  public int hashCode() {
    // simply use a prime number
    // may be need a more balance hash function
    return row * 37 + column;
  }

  public int compareTo(Object o) {
    int thisRow = this.row;
    int thatRow = ((BlockID) o).row;
    int thisColumn = this.column;
    int thatColumn = ((BlockID) o).column;

    if (thisRow != thatRow) {
      return (thisRow < thatRow ? -1 : 1);
    } else {
      return (thisColumn < thatColumn ? -1 : (thisColumn == thatColumn ? 0 : 1));
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null)
      return false;
    if (!(o instanceof BlockID))
      return false;
    return compareTo(o) == 0;
  }

  public byte[] getBytes() {
    return Bytes.toBytes(this.toString());
  }
}
