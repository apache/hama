/**
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
package org.apache.hama.bsp;

/**
 * Contants used in both Client and Application Master
 */
public class YARNBSPConstants {

  /**
   * Environment key name pointing to the hama-yarn's location
   */
  public static final String HAMA_YARN_LOCATION = "HAMAYARNJARLOCATION";

  /**
   * Environment key name denoting the file content length for the hama-yarn application.
   * Used to validate the local resource.
   */
  public static final String HAMA_YARN_SIZE = "HAMAYARNJARSIZE";

  /**
   * Environment key name denoting the file timestamp for the hama-yarn application.
   * Used to validate the local resource.
   */
  public static final String HAMA_YARN_TIMESTAMP = "HAMAYARNJARTIMESTAMP";

  /**
   * Environment key name pointing to the hama release's location
   */
  public static final String HAMA_LOCATION = "HAMALOCATION";

  /**
   * Environment key name denoting the file content length for the hama release.
   * Used to validate the local resource.
   */
  public static final String HAMA_RELEASE_SIZE = "HAMARELEASESIZE";

  /**
   * Environment key name denoting the file timestamp for the hama release.
   * Used to validate the local resource.
   */
  public static final String HAMA_RELEASE_TIMESTAMP = "HAMARELEASETIMESTAMP";

  /**
   * Symbolic link name for application master's jar file in container local resource
   */
  public static final String APP_MASTER_JAR_PATH = "AppMaster.jar";

}
