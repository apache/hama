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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.log4j.Logger;

public abstract class InputFormatBase extends MatrixMapReduce implements
    InputFormat<HStoreKey, MapWritable>, JobConfigurable {
  static final Logger LOG = Logger.getLogger(InputFormatBase.class.getName());

  private Text m_tableName;
  Text[] m_cols;
  HTable m_table;

  public abstract RecordReader<HStoreKey, MapWritable> getRecordReader(
      InputSplit split, JobConf job, Reporter report) throws IOException;

  /**
   * @see org.apache.hadoop.mapred.InputFormat#getSplits(org.apache.hadoop.mapred.JobConf, int)
   */
  @SuppressWarnings("unused")
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Cell meta = m_table.get(Constants.METADATA, Constants.METADATA_ROWS);

    if (bytesToInt(meta.getValue()) < numSplits) {
      numSplits = bytesToInt(meta.getValue());
    }

    Text[] startKeys = new Text[numSplits];
    int interval = bytesToInt(meta.getValue()) / numSplits;

    for (int i = 0; i < numSplits; i++) {
      int row = (i * interval);
      startKeys[i] = new Text(String.valueOf(row));
    }

    InputSplit[] splits = new InputSplit[startKeys.length];
    for (int i = 0; i < startKeys.length; i++) {
      splits[i] = new MatrixSplit(m_tableName, startKeys[i],
          ((i + 1) < startKeys.length) ? startKeys[i + 1] : new Text());
    }
    return splits;
  }

  public void configure(JobConf job) {
    Path[] tableNames = job.getInputPaths();
    m_tableName = new Text(tableNames[0].getName());
    try {
      m_table = new HTable(new HBaseConfiguration(job), m_tableName);
    } catch (IOException e) {
      LOG.info(e);
    }
  }

  public void validateInput(JobConf job) throws IOException {
    Path[] tableNames = job.getInputPaths();
    if (tableNames == null || tableNames.length > 1) {
      throw new IOException("expecting one table name");
    }
  }
}
