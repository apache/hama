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
package org.apache.hama.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

public class MatrixSplit implements InputSplit {
  private Text m_tableName;
  private Text m_startRow;
  private Text m_endRow;

  /** default constructor */
  public MatrixSplit() {
    m_tableName = new Text();
    m_startRow = new Text();
    m_endRow = new Text();
  }

  /**
   * Constructor
   * 
   * @param tableName
   * @param startRow
   * @param endRow
   */
  public MatrixSplit(Text tableName, Text startRow, Text endRow) {
    this();
    m_tableName.set(tableName);
    m_startRow.set(startRow);
    m_endRow.set(endRow);
  }

  /** @return table name */
  public Text getTableName() {
    return m_tableName;
  }

  /** @return starting row key */
  public Text getStartRow() {
    return m_startRow;
  }

  /** @return end row key */
  public Text getEndRow() {
    return m_endRow;
  }

  /** {@inheritDoc} */
  public long getLength() {
    return 0;
  }

  /** {@inheritDoc} */
  public String[] getLocations() {
    return new String[] {};
  }

  /** {@inheritDoc} */
  public void readFields(DataInput in) throws IOException {
    m_tableName.readFields(in);
    m_startRow.readFields(in);
    m_endRow.readFields(in);
  }

  /** {@inheritDoc} */
  public void write(DataOutput out) throws IOException {
    m_tableName.write(out);
    m_startRow.write(out);
    m_endRow.write(out);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return m_tableName + "," + m_startRow + "," + m_endRow;
  }
}