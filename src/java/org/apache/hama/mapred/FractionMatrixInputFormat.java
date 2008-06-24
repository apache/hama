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
import java.util.Map;

import org.apache.hadoop.hbase.HStoreKey;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hama.Constants;

public class FractionMatrixInputFormat extends InputFormatBase implements
    InputFormat<HStoreKey, MapWritable>, JobConfigurable {

  Text[] m_cols = new Text[] { Constants.DENOMINATOR, Constants.NUMERATOR,
      Constants.ORIGINAL };

  public RecordReader<HStoreKey, MapWritable> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    MatrixSplit tSplit = (MatrixSplit) split;

    Text start = tSplit.getStartRow();
    Text end = tSplit.getEndRow();

    return new FractionMatrixReader(start, end, m_table, m_cols);
  }

  class FractionMatrixReader extends MatrixRecordReader {

    public FractionMatrixReader(Text startRow, Text endRow, HTable table,
        Text[] cols) throws IOException {
      super(startRow, endRow, table, cols);
    }

    public boolean next(HStoreKey key, MapWritable value) throws IOException {
      m_row.clear();
      HStoreKey tKey = key;

      boolean hasMore = m_scanner.next(tKey, m_row);
      if (hasMore) {
        value.clear();
        for (Map.Entry<Text, byte[]> e : m_row.entrySet()) {
          if (e.getKey().toString().startsWith(Constants.ORIGINAL.toString())) {
            value.put(e.getKey(), new ImmutableBytesWritable(e.getValue()));
          }
        }
      }
      return hasMore;
    }
  }
}
