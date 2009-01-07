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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public abstract class TableInputFormatBase {
  private static final Log LOG = LogFactory.getLog(TableInputFormatBase.class);
  protected byte[][] inputColumns;
  protected HTable table;
  protected RowFilterInterface rowFilter;
  protected static int repeat;
  
  /**
   * space delimited list of columns
   */
  public static final String COLUMN_LIST = "hama.mapred.tablecolumns";
  public static final String REPEAT_NUM = "hama.mapred.repeat";
  
  public void configure(JobConf job) {
    Path[] tableNames = FileInputFormat.getInputPaths(job);
    if(job.get(REPEAT_NUM) != null) {
      setRepeat(Integer.parseInt(job.get(REPEAT_NUM)));
    }
    String colArg = job.get(COLUMN_LIST);
    String[] colNames = colArg.split(" ");
    byte[][] m_cols = new byte[colNames.length][];
    for (int i = 0; i < m_cols.length; i++) {
      m_cols[i] = Bytes.toBytes(colNames[i]);
    }
    setInputColums(m_cols);
    try {
      setHTable(new HTable(new HBaseConfiguration(job), tableNames[0].getName()));
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  private void setRepeat(int parseInt) {
    repeat =  parseInt;
  }

  public void validateInput(JobConf job) throws IOException {
    // expecting exactly one path
    Path[] tableNames = FileInputFormat.getInputPaths(job);
    if (tableNames == null || tableNames.length > 1) {
      throw new IOException("expecting one table name");
    }

    // expecting at least one column
    String colArg = job.get(COLUMN_LIST);
    if (colArg == null || colArg.length() == 0) {
      throw new IOException("expecting at least one column");
    }
  }

  /**
   * Calculates the splits that will serve as input for the map tasks.
   * <ul>
   * Splits are created in number equal to the smallest between numSplits and
   * the number of {@link HRegion}s in the table. If the number of splits is
   * smaller than the number of {@link HRegion}s then splits are spanned across
   * multiple {@link HRegion}s and are grouped the most evenly possible. In the
   * case splits are uneven the bigger splits are placed first in the
   * {@link InputSplit} array.
   *
   * @param job the map task {@link JobConf}
   * @param numSplits a hint to calculate the number of splits (mapred.map.tasks).
   *
   * @return the input splits
   *
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)
   */
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    byte [][] startKeys = null;
    try {
      startKeys = this.table.getStartKeys();
    } catch (NullPointerException e) { }
    
    if (startKeys == null || startKeys.length == 0) {
      throw new IOException("Expecting at least one region");
    }
    if (this.table == null) {
      throw new IOException("No table was provided");
    }
    if (this.inputColumns == null || this.inputColumns.length == 0) {
      throw new IOException("Expecting at least one column");
    }
    int realNumSplits = numSplits > startKeys.length? startKeys.length:
      numSplits;
    InputSplit[] splits = new InputSplit[realNumSplits];
    int middle = startKeys.length / realNumSplits;
    int startPos = 0;
    for (int i = 0; i < realNumSplits; i++) {
      int lastPos = startPos + middle;
      lastPos = startKeys.length % realNumSplits > i ? lastPos + 1 : lastPos;
      String regionLocation = table.getRegionLocation(startKeys[startPos]).
        getServerAddress().getHostname(); 
      splits[i] = new TableSplit(this.table.getTableName(),
        startKeys[startPos], ((i + 1) < realNumSplits) ? startKeys[lastPos]:
          HConstants.EMPTY_START_ROW, regionLocation);
      LOG.info("split: " + i + "->" + splits[i]);
      startPos = lastPos;
    }
    return splits;
  }


  /**
   * @param inputColumns to be passed to the map task.
   */
  protected void setInputColums(byte[][] inputColumns) {
    this.inputColumns = inputColumns;
  }

  /**
   * Allows subclasses to set the {@link HTable}.
   * 
   * @param table to get the data from
   */
  protected void setHTable(HTable table) {
    this.table = table;
  }

  /**
   * Allows subclasses to set the {@link RowFilterInterface} to be used.
   * 
   * @param rowFilter
   */
  protected void setRowFilter(RowFilterInterface rowFilter) {
    this.rowFilter = rowFilter;
  }
}
