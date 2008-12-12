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

import java.io.IOException;

import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.util.BytesUtil;

public class VectorInputFormat extends TableInputFormatBase implements
    InputFormat<IntWritable, VectorWritable>, JobConfigurable {
  private TableRecordReader tableRecordReader;
  
  /**
   * Iterate over an HBase table data, return (IntWritable, VectorWritable) pairs
   */
  protected static class TableRecordReader extends TableRecordReaderBase
      implements RecordReader<IntWritable, VectorWritable> {

    /**
     * @return IntWritable
     * 
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    public IntWritable createKey() {
      return new IntWritable();
    }

    /**
     * @return VectorWritable
     * 
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    public VectorWritable createValue() {
      return new VectorWritable();
    }

    /**
     * @param key IntWritable as input key.
     * @param value VectorWritable as input value
     * 
     * Converts Scanner.next() to IntWritable, VectorWritable
     * 
     * @return true if there was more data
     * @throws IOException
     */
    public boolean next(IntWritable key, VectorWritable value)
        throws IOException {
      RowResult result = this.scanner.next();
      boolean hasMore = result != null && result.size() > 0;
      if (hasMore) {
        key.set(BytesUtil.bytesToInt(result.getRow()));
        Writables.copyWritable(result, value);
      }
      return hasMore;
    }
  }
  
  /**
   * Builds a TableRecordReader. If no TableRecordReader was provided, uses the
   * default.
   * 
   * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(InputSplit,
   *      JobConf, Reporter)
   */
  public RecordReader<IntWritable, VectorWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    TableSplit tSplit = (TableSplit) split;
    TableRecordReader trr = this.tableRecordReader;
    // if no table record reader was provided use default
    if (trr == null) {
      trr = new TableRecordReader();
    }
    trr.setStartRow(tSplit.getStartRow());
    trr.setEndRow(tSplit.getEndRow());
    trr.setHTable(this.table);
    trr.setInputColumns(this.inputColumns);
    trr.setRowFilter(this.rowFilter);
    trr.init();
    return trr;
  }
  
  /**
   * Allows subclasses to set the {@link TableRecordReader}.
   * 
   * @param tableRecordReader to provide other {@link TableRecordReader}
   *                implementations.
   */
  protected void setTableRecordReader(TableRecordReader tableRecordReader) {
    this.tableRecordReader = tableRecordReader;
  }
}
