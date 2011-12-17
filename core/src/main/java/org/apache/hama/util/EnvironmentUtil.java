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
package org.apache.hama.util;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.util.Shell;

/**
 * This class is responsible to get usernames and groups.
 * 
 */
public class EnvironmentUtil {

  public static String getUnixUserName() throws IOException {
    String jvmName = System.getProperty("user.name");
    if (jvmName == null) {
      String[] result = executeShellCommand(new String[] { Shell.USER_NAME_COMMAND });
      if (result.length != 1) {
        throw new IOException("Expect one token as the result of "
            + Shell.USER_NAME_COMMAND + ": " + toString(result));
      }
      return result[0];
    } else {
      return jvmName;
    }
  }

  public static String getUnixUserGroupName(String user) throws IOException {
    String[] result = executeShellCommand(new String[] { "bash", "-c",
        "id -Gn " + user });
    if (result.length < 1) {
      throw new IOException("Expect one token as the result of "
          + "bash -c id -Gn " + user + ": " + toString(result));
    }
    return result[0];
  }

  public static String[] executeShellCommand(String[] command)
      throws IOException {
    String groups = Shell.execCommand(command);
    StringTokenizer tokenizer = new StringTokenizer(groups);
    int numOfTokens = tokenizer.countTokens();
    String[] tokens = new String[numOfTokens];
    for (int i = 0; tokenizer.hasMoreTokens(); i++) {
      tokens[i] = tokenizer.nextToken();
    }

    return tokens;
  }

  public static String toString(String[] strArray) {
    if (strArray == null || strArray.length == 0) {
      return "";
    }
    StringBuilder buf = new StringBuilder(strArray[0]);
    for (int i = 1; i < strArray.length; i++) {
      buf.append(' ');
      buf.append(strArray[i]);
    }
    return buf.toString();
  }

}
