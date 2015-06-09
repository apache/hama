/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.manager.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * UI Template utility for Hama Manager.
 */
public class UITemplate {

  public static final Log LOG = LogFactory.getLog(UITemplate.class);

  private final static String AREA_BEGIN_OPEN_TAG = "<!--{area,begin,";
  private final static String AREA_END_OPEN_TAG = "<!--{area,end,";
  private final static String AREA_CLOSE_TAG = "}-->";
  private final static String[] ERROR_MESSAGE = {
      "[Error] The template File not exist. ",
      "[Error] The area does not exist. ",
      "[Error] <!--{area,end,..}--> does not exist. ",
      "[Error] <!--{area,begin,..}--> does not exist. " };

  /**
   * read template file contents
   * 
   * @param template fileName
   */
  public String load(String fileName) {

    File tempalteFile = new File(fileName);

    if (tempalteFile.isFile()) {
      return loadTemplateFile(fileName);
    } else {
      return loadTemplateResource(fileName);
    }
  }

  /**
   * read template file contents
   * 
   * @param template fileName
   */
  protected String loadTemplateFile(String fileName) {

    FileInputStream fis = null;
    int bytesRemain; // the number of remaining bytes
    StringBuilder sb = new StringBuilder();

    try {
      fis = new FileInputStream(fileName);

      while ((bytesRemain = fis.available()) > 0) {
        byte[] data = new byte[bytesRemain];
        int bytesRead = fis.read(data);
        if (bytesRead == -1) {
          break;
        }
        sb.append(new String(data));
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return sb.toString();
  }

  /**
   * read template file contents
   * 
   * @param template fileName
   */
  protected String loadTemplateResource(String fileName) {

    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = this.getClass().getClassLoader();
    }

    InputStream is = classLoader.getResourceAsStream(fileName);
    InputStreamReader isr = new InputStreamReader(is);
    BufferedReader br = new BufferedReader(isr);
    StringBuffer sb = new StringBuffer();
    String line;

    try {
      while ((line = br.readLine()) != null) {
        sb.append(line);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (isr != null) {
        try {
          isr.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (is != null) {
        try {
          is.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return sb.toString();
  }

  /**
   * Get the data corresponding to the area in the template
   * 
   * @param all template file data
   * @param areaName area name
   */
  public String getArea(String data, String areaName) {
    return getAreaTemplate(data, areaName);
  }

  /**
   * Get the data corresponding to the area in the template
   * 
   * @param data template file data
   * @param areaName area name
   */
  protected String getAreaTemplate(String data, String areaName) {

    String beginTag = AREA_BEGIN_OPEN_TAG + areaName + AREA_CLOSE_TAG;
    String endTag = AREA_END_OPEN_TAG + areaName + AREA_CLOSE_TAG;

    int beginIndex = data.indexOf(beginTag);
    if (beginIndex == -1) {
      return ERROR_MESSAGE[0] + areaName;
    }
    int endIndex = data.indexOf(endTag);
    if (endIndex == -1) {
      return ERROR_MESSAGE[2] + areaName;
    }
    return data.substring(beginIndex + beginTag.length(), endIndex);
  }

  /**
   * Replace the value of the variable in the data area
   * 
   * @param area area data
   * @param vars variables
   */
  public String convert(String area, HashMap<String, String> vars) {
    return convertVariable(area, vars);
  }

  /**
   * Replace the value of the variable in the data area
   * 
   * @param area area data
   * @param variables variables
   */
  public String convertVariable(String area, HashMap<String, String> variables) {

    StringBuffer sb = new StringBuffer();
    String variableName, variableValue;

    for (int i = 0; i < area.length(); i++) {
      if (area.charAt(i) == '$') {
        variableName = getVariableName(area, i);

        if (variableName == null) {
          sb.append(area.charAt(i));
        } else {
          variableValue = variables.get(variableName);
          if (variableValue != null) {
            sb.append(variableValue);
          }
          i += variableName.length() + 2;
        }
      } else {
        sb.append(area.charAt(i));
      }
    }

    return sb.toString();
  }

  /**
   * Replace the value of the variable in the data area
   * 
   * @param area area data
   * @param pos position to start the conversion
   */
  private String getVariableName(String area, int pos) {
    StringBuffer variableName = new StringBuffer("");
    boolean inVariableName = false;

    for (int i = pos + 1; i < area.length(); i++) {
      char ch = area.charAt(i);
      if ((ch != '{') && (!inVariableName)) {
        return null;
      }
      if (inVariableName) {
        if (ch == '}') {
          return variableName.toString();
        }
        variableName.append(ch);
      }
      inVariableName = true;
    }
    return null;
  }

}
