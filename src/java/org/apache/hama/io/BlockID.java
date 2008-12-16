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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.WritableComparable;

/** A WritableComparable for BlockIDs. */
@SuppressWarnings("unchecked")
public class BlockID implements WritableComparable, java.io.Serializable {
  private static final long serialVersionUID = 6434651179475226613L;
  private int row;
  private int column;

  public BlockID() {
  }

  public BlockID(int row, int column) {
    set(row, column);
  }

  public BlockID(byte[] bytes) throws IOException {
    ByteArrayInputStream bos = new ByteArrayInputStream(bytes);
    ObjectInputStream oos = new ObjectInputStream(bos);
    Object obj = null;
    try {
      obj = oos.readObject();
      this.row = ((BlockID)obj).getRow();
      this.column = ((BlockID)obj).getColumn();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    oos.close();
    bos.close();
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
    row = in.readInt();
    column = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(row);
    // out.write(column);
    out.writeInt(column);
  }

  /**
   * Make BlockID's string representation be same format.
   */
  public String toString() {
    return row + "," + column;
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
    if(o == null) return false;
    if(!(o instanceof BlockID)) return false;
    return compareTo(o) == 0;
  }

  public byte[] getBytes() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(this);
    oos.flush();
    oos.close();
    bos.close();
    byte[] data = bos.toByteArray();
    return data;
  }
}
