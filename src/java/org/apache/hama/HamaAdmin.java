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

public interface HamaAdmin {

  /**
   * Saves matrix as name 'AliaseName'
   *
   * @param matrix
   * @param aliaseName
   * @return true if it saved
   */
  public boolean save(Matrix matrix, String aliaseName); 

  /**
   * @param name
   * @return return a physical path of matrix
   */
  public String getPath(String name);

  /**
   * @param matrixName
   * @return true if matrix is exist
   */
  public boolean matrixExists(String matrixName);

  /**
   * Deletes matrix
   * 
   * @param matrixName
   * @throws IOException
   */
  public void delete(String matrixName) throws IOException;

}
