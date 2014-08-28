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
package org.apache.hama.manager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.SocketException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.BSPMaster;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.GroomServerStatus;
import org.apache.hama.bsp.JobStatus;
import org.apache.hama.manager.util.UITemplate;

/**
 * Log viewer class for for BSP Server.
 */
public class LogView {

  /**
   * get log file list
   * 
   * @param directoryName directory Name
   */
  public static List<File> getLogFileList(String directoryName) {

    List<File> list = new ArrayList<File>();
    File dir = new File(directoryName);

    if (dir.isDirectory()) {

      File[] files = dir.listFiles();

      Arrays.sort(files, new Comparator<File>() {
        @Override
        public int compare(File file1, File file2) {
          return (int)(file2.lastModified() - file1.lastModified());
        }
      });

      for (int i = 0; i < files.length; i++) {
        list.add(files[i]);

      }
    }

    return list;
  }

  /**
   * Returns by changing to a string that is easy to read bytes, KB, MB, GB, TB,
   * etc. of the filesize type size.
   * 
   * @param filesize Change filesize.
   * @return The modified string
   * @see DecimalFormat
   */
  public static String convertFileSize(long filesize) {
    double size = filesize;
    String unit = "bytes";
    String format = "#,###.## ";

    if (1048576 > filesize && filesize >= 1024) {
      size = size / 1024;
      unit = "KB";
    } else if (1073741824 > filesize && filesize >= 1048576) {
      size = size / 1048576;
      unit = "MB";
    } else if (1099511627776L > filesize && filesize >= 1073741824) {
      size = size / 1073741824;
      unit = "GB";
    } else if (filesize >= 1099511627776L) {
      size = size / 1099511627776L;
      unit = "TB";
    }

    return new DecimalFormat(format).format(size) + unit;
  }

  /**
   * download log file.
   * 
   * @param filPath log file path.
   * @throws Exception
   */
  public static void downloadFile(HttpServletResponse response, String filePath)
      throws ServletException, IOException {

    File file = new File(filePath);

    if (!file.exists()) {
      throw new ServletException("File doesn't exists.");
    }

    String headerKey = "Content-Disposition";
    String headerValue = "attachment; filename=\"" + file.getName() + "\"";

    BufferedInputStream in = null;
    BufferedOutputStream out = null;

    try {

      response.setContentType("application/octet-stream");
      response.setContentLength((int) file.length());
      response.setHeader(headerKey, headerValue);

      in = new BufferedInputStream(new FileInputStream(file));
      out = new BufferedOutputStream(response.getOutputStream());

      byte[] buffer = new byte[4 * 1024];
      int read = -1;

      while ((read = in.read(buffer)) != -1) {
        out.write(buffer, 0, read);
      }

    } catch (SocketException e) {
      // download cancel..
    } catch (Exception e) {
      response.reset();
      response.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.toString());
    } finally {
      if (in != null) {
        in.close();
      }
      if (out != null) {
        out.flush();
        out.close();
      }
    }
  }

  /**
   * A servlet implementation
   */
  public static class Servlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    public static final Log LOG = LogFactory.getLog(Servlet.class);

    public static final String USAGES = "\nUSAGES:\n";

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {

      final String targetUri = request.getRequestURI();

      String dirName = request.getParameter("dir");
      String fileName = request.getParameter("file");
      String pageType = request.getParameter("type");
      pageType = (pageType == null) ? "list" : pageType;
      dirName = (dirName == null) ? "" : dirName;

      String hamaLogDir = getLogHomeDir();
      String logDirPath;

      if (dirName == "") {
        logDirPath = hamaLogDir;
      } else { // Do not access upper path ('/..'', '..', '../' )
        dirName = dirName.replaceAll("\\.\\.[/]|\\.\\.[//]|\\.\\.", "");
        logDirPath = hamaLogDir + "/" + dirName;
      }

      String logfilePath = logDirPath + "/" + fileName;

      if (pageType.equals("download")) { // download log file

        LogView.downloadFile(response, logfilePath);

      } else { // log viewer 

        response.setContentType("text/html");
        PrintWriter out = response.getWriter();

        UITemplate uit = new UITemplate();
        String tplPath = "webapp/commons/tpl/"; // UI tempalet file path
        String tplfile = uit.load(tplPath + "tpl.logview.html"); // UI tmepalte
                                                                 // file

        HashMap<String, String> vars = new HashMap<String, String>();

        String tplHead = uit.getArea(tplfile, "head");
        String tplTail = uit.getArea(tplfile, "tail");

        vars.put("title", "logview");
        vars.put("hamaLogDir", hamaLogDir);

        out.println(uit.convert(tplHead, vars));
        vars.clear();

        if (pageType.equals("list")) { // list 

          String[] listArea = new String[4];
          listArea[0] = uit.getArea(tplfile, "list0");
          listArea[1] = uit.getArea(tplfile, "list1");
          listArea[2] = uit.getArea(tplfile, "list2");
          listArea[3] = uit.getArea(tplfile, "list3");

          vars.put("hamaLogDir", hamaLogDir);
          vars.put("dirName", dirName);
          vars.put("targetUri", targetUri);
          out.println(uit.convert(listArea[0], vars));
          vars.clear();

          List<File> arrayList = LogView.getLogFileList(logDirPath);
          File file;

          for (int i = 0; i < arrayList.size(); i++) {
            file = arrayList.get(i);
            vars.put("dirName", dirName);
            vars.put("fileName", file.getName());
            vars.put("fileLastModified",
                new Date(file.lastModified()).toString());
            vars.put("targetUri", targetUri);

            if (file.isDirectory()) {
              vars.put("type", "dir");
              out.println(uit.convert(listArea[1], vars));
            } else {
              vars.put("fileLength", LogView.convertFileSize(file.length()));
              vars.put("type", "file");
              out.println(uit.convert(listArea[2], vars));
            }
          }

          vars.clear();
          out.println(uit.convert(listArea[3], vars));

        } else if (pageType.equals("detail")) { // detail 

          String[] detailArea = new String[4];
          detailArea[0] = uit.getArea(tplfile, "detail0");
          detailArea[1] = uit.getArea(tplfile, "detail1");
          detailArea[2] = uit.getArea(tplfile, "detail2");

          vars.put("dirName", dirName);
          vars.put("fileName", fileName);
          vars.put("hamaLogDir", hamaLogDir);
          vars.put("targetUri", targetUri);

          out.println(uit.convert(detailArea[0], vars));

          BufferedReader br = null;
          String logLine;

          try {
            br = new BufferedReader(new FileReader(logfilePath));

            while ((logLine = br.readLine()) != null) {
              vars.put("logLine", logLine);
              out.println(uit.convert(detailArea[1], vars));
            }

          } catch (IOException e) {
            e.printStackTrace();
          } finally {
            if (br != null) {
              br.close();
            }

          }

          out.println(uit.convert(detailArea[2], vars));

        } else if (pageType.equals("tail")) { // tail 
          String tailLine = request.getParameter("tailLine");
          tailLine = (tailLine == null) ? "100" : tailLine;

          String[] tailArea = new String[3];
          tailArea[0] = uit.getArea(tplfile, "logtail0");
          tailArea[1] = uit.getArea(tplfile, "logtail1");
          tailArea[2] = uit.getArea(tplfile, "logtail2");

          vars.put("hamaLogDir", hamaLogDir);
          vars.put("dirName", dirName);
          vars.put("fileName", fileName);
          vars.put("tailLine", tailLine);
          vars.put("pageType", pageType);
          vars.put("targetUri", targetUri);

          out.println(uit.convert(tailArea[0], vars));
          vars.clear();

          InputStream is = null;
          InputStreamReader isr = null;
          BufferedReader br = null;

          try {

            String tailcmd = "tail -" + tailLine + "  " + logfilePath;
            Runtime runtime = Runtime.getRuntime();
            Process process = runtime.exec(tailcmd);

            is = process.getInputStream();
            isr = new InputStreamReader(is);
            br = new BufferedReader(isr);
            String eachLine = "";

            int lineNum = 0;
            while ((eachLine = br.readLine()) != null) {
              vars.put("lineNum", Integer.toString(++lineNum));
              vars.put("logLine", eachLine);
              out.println(uit.convert(tailArea[1], vars));
            }

          } catch (IOException e) {
            e.printStackTrace();
          } finally {
            if (br != null) {
              br.close();
            }
            if (isr != null) {
              isr.close();
            }
            if (is != null) {
              is.close();
            }
          }

          vars.clear();
          out.println(uit.convert(tailArea[2], vars));

        } else if (pageType.equals("tasklist")) { // job list
          String jobId = request.getParameter("jobId");
          String[] listArea = new String[3];
          listArea[0] = uit.getArea(tplfile, "tasklist0");
          listArea[1] = uit.getArea(tplfile, "tasklist1");
          listArea[2] = uit.getArea(tplfile, "tasklist2");

          ServletContext ctx = getServletContext();
          BSPMaster tracker = (BSPMaster) ctx.getAttribute("bsp.master");
          ClusterStatus status = tracker.getClusterStatus(true);
          JobStatus jobStatus = tracker.getJobStatus(BSPJobID.forName(jobId));

          vars.put("hamaLogDir", hamaLogDir);
          vars.put("dirName", dirName);
          vars.put("targetUri", targetUri);
          vars.put("jobId", jobId);
          vars.put("jobStatus", jobStatus.getState().toString());
          vars.put("jobName", jobStatus.getName());

          out.println(uit.convert(listArea[0], vars));
          vars.clear();

          for (Entry<String, GroomServerStatus> entry : status
              .getActiveGroomServerStatus().entrySet()) {
            vars.put("jobId", jobId);
            vars.put("dirName", dirName);
            vars.put("serverName", entry.getKey());
            vars.put("hostName", entry.getValue().getGroomHostName());
            vars.put("targetUri", targetUri);
            vars.put("type", "dir");
            out.println(uit.convert(listArea[1], vars));

          }

          vars.clear();
          out.println(uit.convert(listArea[2], vars));
        }

        out.println(tplTail);
      }

    }

    /**
     * Get Hama log Directory
     * @return hama log directory
     */
    private static String getLogHomeDir() {
      Map<String, String> env = System.getenv();
      String hamaHome = env.get("HAMA_HOME");
      String hamaLogDir = null;

      if (!env.containsKey("HAMA_LOG_DIR")) {
        hamaLogDir = hamaHome + "/logs";
      } else {
        hamaLogDir = env.get("HAMA_LOG_DIR");
      }

      return hamaLogDir;
    }
  }

}
