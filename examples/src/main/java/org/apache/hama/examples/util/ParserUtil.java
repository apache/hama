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
package org.apache.hama.examples.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Option;

/**
 * Facilitate the command line argument parsing.
 * 
 */
public class ParserUtil {

  /**
   * Parse and return the string parameter.
   * 
   * @param cli
   * @param option
   * @return
   */
  public static String getString(CommandLine cli, Option option) {
    Object val = cli.getValue(option);
    if (val != null) {
      return val.toString();
    }
    return null;
  }

  /**
   * Parse and return the integer parameter.
   * 
   * @param cli
   * @param option
   * @return
   */
  public static Integer getInteger(CommandLine cli, Option option) {
    Object val = cli.getValue(option);
    if (val != null) {
      return Integer.parseInt(val.toString());
    }
    return null;
  }

  /**
   * Parse and return the long parameter.
   * 
   * @param cli
   * @param option
   * @return
   */
  public static Long getLong(CommandLine cli, Option option) {
    Object val = cli.getValue(option);
    if (val != null) {
      return Long.parseLong(val.toString());
    }
    return null;
  }

  /**
   * Parse and return the double parameter.
   * 
   * @param cli
   * @param option
   * @return
   */
  public static Double getDouble(CommandLine cli, Option option) {
    Object val = cli.getValue(option);
    if (val != null) {
      return Double.parseDouble(val.toString());
    }
    return null;
  }

  /**
   * Parse and return the boolean parameter. If the parameter is set, it is
   * true, otherwise it is false.
   * 
   * @param cli
   * @param option
   * @return
   */
  public static boolean getBoolean(CommandLine cli, Option option) {
    return cli.hasOption(option);
  }
  
  /**
   * Parse and return the array parameters.
   * @param cli
   * @param option
   * @return
   */
  public static List<String> getStrings(CommandLine cli, Option option) {
    List<String> list = new ArrayList<String>();
    for (Object obj : cli.getValues(option)) {
      list.add(obj.toString());
    }
    return list;
  }

  /**
   * Parse and return the array parameters.
   * @param cli
   * @param option
   * @return
   */
  public static List<Integer> getInts(CommandLine cli, Option option) {
    List<Integer> list = new ArrayList<Integer>();
    for (String str : getStrings(cli, option)) {
      list.add(Integer.parseInt(str));
    }
    return list;
  }
}

