/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hama.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.mapred.TableSplit;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hama.Constants;
import org.apache.hama.DenseMatrix;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.BlockPosition;
import org.apache.hama.io.VectorWritable;

/**
 * A Map/Reduce help class for blocking a DenseMatrix to a block-formated matrix
 */
public class BlockingMapRed {
  static final Log LOG = LogFactory.getLog(BlockingMapRed.class);
  /** Parameter of the path of the matrix to be blocked * */
  public static final String BLOCKING_MATRIX = "hama.blocking.matrix";

  /**
   * Initialize a job to blocking a table
   * 
   * @param matrixPath
   * @param job
   */
  public static void initJob(String matrixPath, JobConf job) {
    job.setMapperClass(BlockingMapper.class);
    FileInputFormat.addInputPaths(job, matrixPath);

    job.setInputFormat(MyInputFormat.class);
    job.setMapOutputKeyClass(BlockID.class);
    job.setMapOutputValueClass(VectorWritable.class);
    job.setOutputFormat(NullOutputFormat.class);

    job.set(BLOCKING_MATRIX, matrixPath);
    job.set(VectorInputFormat.COLUMN_LIST, Constants.BLOCK_POSITION);
  }

  /**
   * Abstract Blocking Map/Reduce Class to configure the job.
   */
  public static abstract class BlockingMapRedBase extends MapReduceBase {
    protected DenseMatrix matrix;

    @Override
    public void configure(JobConf job) {
      try {
        matrix = new DenseMatrix(new HamaConfiguration(), job.get(
            BLOCKING_MATRIX, ""));
      } catch (IOException e) {
        LOG.warn("Load matrix_blocking failed : " + e.getMessage());
      }
    }
  }

  /**
   * Mapper Class
   */
  public static class BlockingMapper extends BlockingMapRedBase implements
      Mapper<BlockID, BlockPosition, BlockID, VectorWritable> {

    @Override
    public void map(BlockID key, BlockPosition value,
        OutputCollector<BlockID, VectorWritable> output, Reporter reporter)
        throws IOException {
      int startRow = value.getStartRow();
      int endRow = value.getEndRow();
      int startColumn = value.getStartColumn();
      int endColumn = value.getEndColumn();

      matrix.setBlock(key.getRow(), key.getColumn(), matrix.subMatrix(startRow,
          endRow, startColumn, endColumn));
    }
  }

  static class MyInputFormat extends TableInputFormatBase implements
      InputFormat<BlockID, BlockPosition>, JobConfigurable {
    static final Log LOG = LogFactory.getLog(MyInputFormat.class);
    private TableRecordReader tableRecordReader;

    /**
     * Iterate over an HBase table data, return (BlockID, BlockWritable) pairs
     */
    protected static class TableRecordReader extends TableRecordReaderBase
        implements RecordReader<BlockID, BlockPosition> {

      /**
       * @return IntWritable
       * 
       * @see org.apache.hadoop.mapred.RecordReader#createKey()
       */
      public BlockID createKey() {
        return new BlockID();
      }

      /**
       * @return BlockWritable
       * 
       * @see org.apache.hadoop.mapred.RecordReader#createValue()
       */
      public BlockPosition createValue() {
        return new BlockPosition();
      }

      /**
       * @param key BlockID as input key.
       * @param value BlockWritable as input value
       * 
       * Converts Scanner.next() to BlockID, BlockWritable
       * 
       * @return true if there was more data
       * @throws IOException
       */
      public boolean next(BlockID key, BlockPosition value) throws IOException {
        RowResult result = this.scanner.next();
        boolean hasMore = result != null && result.size() > 0;
        if (hasMore) {
          byte[] row = result.getRow();
          BlockID bID = new BlockID(row);
          key.set(bID.getRow(), bID.getColumn());
          Writables.copyWritable(
              new BlockPosition(result.get(Constants.BLOCK_POSITION).getValue()), 
              value);
        }
        return hasMore;
      }
    }

    /**
     * Builds a TableRecordReader. If no TableRecordReader was provided, uses
     * the default.
     * 
     * @see org.apache.hadoop.mapred.InputFormat#getRecordReader(InputSplit,
     *      JobConf, Reporter)
     */
    public RecordReader<BlockID, BlockPosition> getRecordReader(
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
}
