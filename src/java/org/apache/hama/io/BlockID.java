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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/** A WritableComparable for BlockIDs. */
@SuppressWarnings("unchecked")
public class BlockID implements WritableComparable {
  private int row;
  private int column;

  public BlockID() {
  }

  public BlockID(int row, int column) {
    set(row, column);
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

  /**
   * BlockID Comparator 
   */
  /* why we need a special Comparator
  public static class Comparator extends WritableComparator {
    protected Comparator() {
      super(BlockID.class);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int thisRow = readInt(b1, s1);
      int thatRow = readInt(b2, s2);
      // why read from l1 & l2
      // l1 means length 1, and l2 means length 2
      int thisColumn = readInt(b1, l1);
      int thatColumn = readInt(b2, l2);

      if (thisRow != thatRow) {
        return (thisRow < thatRow ? -1 : 1);
      } else {
        return (thisColumn < thatColumn ? -1 : (thisColumn == thatColumn ? 0
            : 1));
      }
    }
  }

  static { // register this comparator
    WritableComparator.define(BlockID.class, new Comparator());
  }
  */
}
