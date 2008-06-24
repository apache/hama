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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormatBase;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hama.Constants;
import org.apache.log4j.Logger;

public class MatrixOutputFormat extends OutputFormatBase<Text, MapWritable> {

  /** JobConf parameter that specifies the output */
  public static final String OUTPUT = "hbase.mapred.output";
  public int rowCount = 0;
  public int columnCount = 0;
  static final Logger LOG = Logger
      .getLogger(MatrixOutputFormat.class.getName());

  /** constructor */
  public MatrixOutputFormat() {
    super();
  }

  /**
   * Convert Reduce output (key, value) to (HStoreKey, KeyedDataArrayWritable)
   * and write to an HBase table
   */
  protected class TableRecordWriter implements RecordWriter<Text, MapWritable> {
    private HTable m_table;

    /**
     * Instantiate a TableRecordWriter with the HBase HClient for writing.
     * 
     * @param table
     */
    public TableRecordWriter(HTable table) {
      m_table = table;
    }

    /** {@inheritDoc} */
    public void close(@SuppressWarnings("unused")
    Reporter reporter) {
      if (rowCount > 0 && columnCount > 0) {
        BatchUpdate b = new BatchUpdate(Constants.METADATA);
        b.put(Constants.METADATA_ROWS, intToBytes(rowCount));
        b.put(Constants.METADATA_COLUMNS, intToBytes(columnCount));
        try {
          m_table.commit(b);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    /**
     * Converts the int to byte array
     * 
     * @param i
     * @return Byte Array
     */
    public byte[] intToBytes(int i) {
      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.order(ByteOrder.nativeOrder());
      bb.putInt(i);
      return bb.array();
    }

    /** {@inheritDoc} */
    public void write(Text key, MapWritable value) throws IOException {

      BatchUpdate b = new BatchUpdate(key);
      b.put(Constants.METADATA_ROWS, intToBytes(rowCount));
      b.put(Constants.METADATA_COLUMNS, intToBytes(columnCount));

      rowCount++;
      LOG.info(columnCount);
      if (columnCount < value.keySet().size())
        columnCount = value.keySet().size();

      for (Map.Entry<Writable, Writable> e : value.entrySet()) {
        b.put((Text) e.getKey(), ((ImmutableBytesWritable) e.getValue()).get());
      }
      m_table.commit(b); // end transaction
    }

  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unchecked")
  public RecordWriter getRecordWriter(FileSystem ignored, JobConf job, 
  String name, Progressable progress) throws IOException {

    // expecting exactly one path

    Text tableName = new Text(job.get(OUTPUT));
    HTable table = null;
    try {
      table = new HTable(new HBaseConfiguration(job), tableName);
    } catch (IOException e) {
      LOG.error(e);
      throw e;
    }
    return new TableRecordWriter(table);
  }

  /** {@inheritDoc} */
  @Override
  @SuppressWarnings("unused")
  public void checkOutputSpecs(FileSystem ignored, JobConf job)
      throws FileAlreadyExistsException, InvalidJobConfException, IOException {

    String tableName = job.get(OUTPUT);
    if (tableName == null) {
      throw new IOException("Must specify table name");
    }
  }
}
