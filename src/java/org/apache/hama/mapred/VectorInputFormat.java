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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.util.BytesUtil;

public class VectorInputFormat extends TableInputFormatBase implements
    InputFormat<IntWritable, VectorWritable>, JobConfigurable {
  static final Log LOG = LogFactory.getLog(VectorInputFormat.class);
  private TableRecordReader tableRecordReader;
  
  /**
   * Iterate over an HBase table data, return (IntWritable, VectorWritable) pairs
   */
  protected static class TableRecordReader extends TableRecordReaderBase
      implements RecordReader<IntWritable, VectorWritable> {

   private int totalRows;
   private int processedRows;

   @Override
   public void init() throws IOException {
     super.init();
     if(endRow.length == 0) { // the last split, we don't know the end row
       totalRows = 0;         // so we just skip it.
     } else {
       if(startRow.length == 0) { // the first split, start row is 0
         totalRows = BytesUtil.bytesToInt(endRow);
       } else {
         totalRows = BytesUtil.bytesToInt(endRow) - BytesUtil.bytesToInt(startRow);
       }
     }
     processedRows = 0;
     LOG.info("Split (" + Bytes.toString(startRow) + ", " + Bytes.toString(endRow) + 
       ") -> " + totalRows);
   }


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
      RowResult result;
      try {
        result = this.scanner.next();
      } catch (UnknownScannerException e) {
        LOG.debug("recovered from " + StringUtils.stringifyException(e));  
        restart(lastRow);
        this.scanner.next();    // skip presumed already mapped row
        result = this.scanner.next();
      }
      
      boolean hasMore = result != null && result.size() > 0;
      if (hasMore) {
        byte[] row = result.getRow();
        key.set(BytesUtil.bytesToInt(row));
        lastRow = row;
        Writables.copyWritable(result, value);
        processedRows++;
      }
      return hasMore;
    }

    @Override
    public float getProgress() {
      if(totalRows <= 0) {
        return 0;
      } else {
        return Math.min(1.0f, processedRows / (float)totalRows);
      }
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
