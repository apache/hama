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

import org.apache.hadoop.io.Writable;

public class BlockPosition implements Writable, java.io.Serializable {
  private static final long serialVersionUID = 3717208691381491714L;
  public int startRow;
  public int endRow;
  public int startColumn;
  public int endColumn;

  public BlockPosition() {
  }

  public BlockPosition(byte[] bytes) throws IOException {
    ByteArrayInputStream bos = new ByteArrayInputStream(bytes);
    ObjectInputStream oos = new ObjectInputStream(bos);
    Object obj = null;
    try {
      obj = oos.readObject();
      this.startRow = ((BlockPosition)obj).startRow;
      this.endRow = ((BlockPosition)obj).endRow;
      this.startColumn = ((BlockPosition)obj).startColumn;
      this.endColumn = ((BlockPosition)obj).endColumn;
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    oos.close();
    bos.close();
  }
  
  public BlockPosition(int startRow, int endRow, int startColumn, int endColumn) {
    this.startRow = startRow;
    this.endRow = endRow;
    this.startColumn = startColumn;
    this.endColumn = endColumn;
  }

  public void readFields(DataInput in) throws IOException {
    this.startRow = in.readInt();
    this.endRow = in.readInt();
    this.startColumn = in.readInt();
    this.endColumn = in.readInt();
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(startRow);
    out.writeInt(endRow);
    out.writeInt(startColumn);
    out.writeInt(endColumn);
  }

  public int getStartRow() {
    return startRow;
  }

  public void setStartRow(int startRow) {
    this.startRow = startRow;
  }

  public int getEndRow() {
    return endRow;
  }

  public void setEndRow(int endRow) {
    this.endRow = endRow;
  }

  public int getStartColumn() {
    return startColumn;
  }

  public void setStartColumn(int startColumn) {
    this.startColumn = startColumn;
  }

  public int getEndColumn() {
    return endColumn;
  }

  public void setEndColumn(int endColumn) {
    this.endColumn = endColumn;
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
