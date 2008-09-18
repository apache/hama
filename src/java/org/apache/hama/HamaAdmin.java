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
package org.apache.hama;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Admin tool
 */
public class HamaAdmin {
  public HamaConfiguration conf;
  public HBaseAdmin admin;
  public HTable table;

  public HamaAdmin(HamaConfiguration conf) {
    this.conf = conf;
    initialJob();
  }

  public HamaAdmin(HamaConfiguration conf, HBaseAdmin admin) {
    this.conf = conf;
    this.admin = admin;
    initialJob();
  }

  public void initialJob() {
    try {
      if (!admin.tableExists(Constants.ADMINTABLE)) {
        HTableDescriptor tableDesc = new HTableDescriptor(Constants.ADMINTABLE);
        tableDesc.addFamily(new HColumnDescriptor(Constants.PATHCOLUMN));
        admin.createTable(tableDesc);
      }

      table = new HTable(conf, Constants.ADMINTABLE);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public boolean put(String matrixName, String path) {
    boolean result = false;

    BatchUpdate update = new BatchUpdate(path);
    update.put(Constants.PATHCOLUMN, Bytes.toBytes(matrixName));
    try {
      table.commit(update);
      result = true;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result;
  }

  public String get(String path) {
    try {
      byte[] result = table.get(path, Constants.PATHCOLUMN).getValue();
      return Bytes.toString(result);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
