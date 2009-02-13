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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.filter.StopRowFilter;

public abstract class HTableRecordReaderBase {
  protected byte[] startRow;
  protected byte[] endRow;
  protected byte [] lastRow;
  protected RowFilterInterface trrRowFilter;
  protected Scanner scanner;
  protected HTable htable;
  protected byte[][] trrInputColumns;

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow
   * @throws IOException
   */
  public void restart(byte[] firstRow) throws IOException {
    if ((endRow != null) && (endRow.length > 0)) {
      if (trrRowFilter != null) {
        final Set<RowFilterInterface> rowFiltersSet =
          new HashSet<RowFilterInterface>();
        rowFiltersSet.add(new StopRowFilter(endRow));
        rowFiltersSet.add(trrRowFilter);
        this.scanner = this.htable.getScanner(trrInputColumns, startRow,
          new RowFilterSet(RowFilterSet.Operator.MUST_PASS_ALL,
            rowFiltersSet));
      } else {
        this.scanner =
          this.htable.getScanner(trrInputColumns, firstRow, endRow);
      }
    } else {
      this.scanner =
        this.htable.getScanner(trrInputColumns, firstRow, trrRowFilter);
    }
  }
  
  /**
   * Build the scanner. Not done in constructor to allow for extension.
   *
   * @throws IOException
   */
  public void init() throws IOException {
    restart(startRow);
  }

  /**
   * @param htable the {@link HTable} to scan.
   */
  public void setHTable(HTable htable) {
    this.htable = htable;
  }

  /**
   * @param inputColumns the columns
   */
  public void setInputColumns(final byte[][] inputColumns) {
    byte[][] columns = inputColumns;
    this.trrInputColumns = columns;
  }

  /**
   * @param startRow the first row in the split
   */
  public void setStartRow(final byte[] startRow) {
    byte[] sRow = startRow;
    this.startRow = sRow;
  }

  /**
   * 
   * @param endRow the last row in the split
   */
  public void setEndRow(final byte[] endRow) {
    byte[] eRow = endRow;
    this.endRow = eRow;
  }

  /**
   * @param rowFilter the {@link RowFilterInterface} to be used.
   */
  public void setRowFilter(RowFilterInterface rowFilter) {
    this.trrRowFilter = rowFilter;
  }

  public void close() throws IOException {
    this.scanner.close();
  }

  public long getPos() {
    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return 0;
  }

  public float getProgress() {
    // Depends on the total number of tuples and getPos
    return 0;
  }

}
