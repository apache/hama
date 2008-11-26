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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;
import org.apache.hama.DenseMatrix;
import org.apache.hama.DenseVector;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.SubMatrix;
import org.apache.hama.io.BlockID;
import org.apache.hama.io.VectorWritable;

/**
 * A Map/Reduce help class for blocking a DenseMatrix to a block-formated matrix
 **/
public class BlockingMapRed {

  static final Log LOG = LogFactory.getLog(BlockingMapRed.class); 
  /** Parameter of the path of the matrix to be blocked **/
  public static final String BLOCKING_MATRIX = "hama.blocking.matrix";
  
  protected static final String TEMP_OUTPUT_DIR = "/tmp/hama/";
  /** 
   * Initialize a job to blocking a table
   * 
   * @param matrixPath
   * @param job
   */
  public static void initJob(String matrixPath, JobConf job) {
    job.setMapperClass(BlockingMapper.class);
    job.setReducerClass(BlockingReducer.class);
    FileInputFormat.addInputPaths(job, matrixPath);
    
    job.setInputFormat(VectorInputFormat.class);
    job.setMapOutputKeyClass(BlockRowId.class);
    job.setMapOutputValueClass(VectorWritable.class);
    
    // TODO: we don't need an output
    FileOutputFormat.setOutputPath(job, new Path(TEMP_OUTPUT_DIR + System.currentTimeMillis()));
    
    job.setOutputValueGroupingComparator(GroupingComparator.class);
    
    job.set(BLOCKING_MATRIX, matrixPath);
    job.set(VectorInputFormat.COLUMN_LIST, Constants.COLUMN);
  }
  
  /**
   * Abstract Blocking Map/Reduce Class to configure the job.
   */
  public static abstract class BlockingMapRedBase extends MapReduceBase {
    
    protected DenseMatrix matrix;
    protected int mBlockNum;
    protected int mBlockRowSize;
    protected int mBlockColSize;
    
    @Override
    public void configure(JobConf job) {
      try {
        matrix = new DenseMatrix(new HamaConfiguration(), job.get(BLOCKING_MATRIX, ""));
        mBlockNum = matrix.getBlockSize();
        mBlockRowSize = matrix.getRows() / mBlockNum;
        mBlockColSize = matrix.getColumns() / mBlockNum;
      } catch (IOException e) {
        LOG.warn("Load matrix_blocking failed : " + e.getMessage());
      }
    }
    
  }
  
  /**
   * Mapper Class
   */
  public static class BlockingMapper extends BlockingMapRedBase implements
      Mapper<IntWritable, VectorWritable, BlockRowId, VectorWritable> {

    @Override
    public void map(IntWritable key, VectorWritable value,
        OutputCollector<BlockRowId, VectorWritable> output, Reporter reporter)
        throws IOException {
      int startColumn;
      int endColumn;
      int blkRow = key.get() / mBlockRowSize;
      DenseVector dv = value.getDenseVector();
      for(int i = 0 ; i < mBlockNum; i++) {
        startColumn = i * mBlockColSize;
        endColumn = startColumn + mBlockColSize - 1;
        output.collect(new BlockRowId(new BlockID(blkRow, i), key.get()), 
            new VectorWritable(key.get(), (DenseVector) dv.subVector(startColumn, endColumn)));
      }
    }

  }
  
  /**
   * Reducer Class
   */
  public static class BlockingReducer extends BlockingMapRedBase implements
      Reducer<BlockRowId, VectorWritable, BlockID, SubMatrix> {

    @Override
    public void reduce(BlockRowId key, Iterator<VectorWritable> values,
        OutputCollector<BlockID, SubMatrix> output, Reporter reporter)
        throws IOException {
      BlockID blkId = key.getBlockId();
      final SubMatrix subMatrix = new SubMatrix(mBlockRowSize, mBlockColSize);
      int i=0, j=0;
      int colBase = blkId.getColumn() * mBlockColSize;
      while(values.hasNext()) {
        if(i > mBlockRowSize) 
          throw new IOException("BlockRowSize dismatched.");
        
        VectorWritable vw = values.next();
        if(vw.size() != mBlockColSize) 
          throw new IOException("BlockColumnSize dismatched.");
        for(j=0; j<mBlockColSize; j++) {
          subMatrix.set(i, j, vw.get(colBase + j));
        }
        i++;
      }
      
      matrix.setBlock(blkId.getRow(), blkId.getColumn(), subMatrix);
      
      // we will not collect anything here.
    }
    
  }
  
  /**
   * A Grouping Comparator to group all the {@link BlockRowId} with 
   * same {@link org.apache.hama.io.BlockID}
   */
  public static class GroupingComparator implements RawComparator<BlockRowId> {

    private final DataInputBuffer buffer = new DataInputBuffer();
    private final BlockRowId key1 = new BlockRowId();
    private final BlockRowId key2 = new BlockRowId();
    
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2,
        int l2) {
      try {
        buffer.reset(b1, s1, l1);       // parse k1
        key1.readFields(buffer);
        
        buffer.reset(b2, s2, l2);
        key2.readFields(buffer);
        
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      
      return compare(key1, key2);
    }

    @Override
    public int compare(BlockRowId first, BlockRowId second) {
      return first.mBlockId.compareTo(second.mBlockId);
    }
    
  }
  
  /**
   * Help Class to store <blockid, rowid> pair.
   */
  public static class BlockRowId implements WritableComparable<BlockRowId> {

    BlockID mBlockId;
    int mRowId;
    
    /**
     * Empty Constructor used for serialization.
     */
    public BlockRowId() { }
    
    /**
     * Construct a block-row id using blockid & rowid
     * @param blockid
     * @param rowid
     */
    public BlockRowId(BlockID blockid, int rowid) {
      mBlockId = blockid;
      mRowId = rowid;
    }
    
    /** 
     * Get the block ID
     * @return BlockID
     */
    public BlockID getBlockId() { return mBlockId; }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      mBlockId = new BlockID();
      mBlockId.readFields(in);
      mRowId = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      mBlockId.write(out);
      out.writeInt(mRowId);
    }

    // first compare block ids
    // when blockids are same, then compare row ids
    @Override
    public int compareTo(BlockRowId another) {
      int cmp = mBlockId.compareTo(another.mBlockId);
      if(cmp == 0) {
        cmp = mRowId - another.mRowId;
      }
      return cmp;
    }

    @Override
    public boolean equals(Object obj) {
      if(obj == null) return false;
      if(!(obj instanceof BlockRowId)) return false;
      
      BlockRowId another = (BlockRowId)obj;
      return compareTo(another) == 0;
    }

    // tricky here
    // we just used the block id to generate the hashcode
    // so that the same block will be in the same reducer
    @Override
    public int hashCode() {
      return mBlockId.hashCode();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("<");
      sb.append(mBlockId);
      sb.append(",");
      sb.append(mRowId);
      sb.append(">");
      return sb.toString();
    }
    
  }

}
