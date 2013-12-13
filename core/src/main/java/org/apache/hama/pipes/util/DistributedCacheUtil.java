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
package org.apache.hama.pipes.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hama.util.DistCacheUtils;

public class DistributedCacheUtil {

  private static final Log LOG = LogFactory.getLog(DistributedCacheUtil.class);

  /**
   * Transfers DistributedCache files into the local cache files. Also creates
   * symbolic links for URIs specified with a fragment if
   * DistributedCache.getSymlinks() is true.
   * 
   * @param conf The job's configuration
   * @throws IOException If a DistributedCache file cannot be found.
   */
  public static final void moveLocalFiles(Configuration conf)
      throws IOException {
    StringBuilder files = new StringBuilder();
    boolean first = true;
    if (DistributedCache.getCacheFiles(conf) != null) {
      for (URI uri : DistributedCache.getCacheFiles(conf)) {
        if (uri != null) {
          if (!first) {
            files.append(",");
          }
          if (null != uri.getFragment() && DistributedCache.getSymlink(conf)) {

            FileUtil.symLink(uri.getPath(), uri.getFragment());
            files.append(uri.getFragment()).append(",");
          }
          FileSystem hdfs = FileSystem.get(conf);
          Path pathSrc = new Path(uri.getPath());
          // LOG.info("pathSrc: " + pathSrc);
          if (hdfs.exists(pathSrc)) {
            LocalFileSystem local = LocalFileSystem.getLocal(conf);
            Path pathDst = new Path(local.getWorkingDirectory(),
                pathSrc.getName());
            // LOG.info("user.dir: "+System.getProperty("user.dir"));
            // LOG.info("WorkingDirectory: "+local.getWorkingDirectory());
            // LOG.info("pathDst: " + pathDst);
            LOG.debug("copyToLocalFile: " + pathDst);
            hdfs.copyToLocalFile(pathSrc, pathDst);
            local.deleteOnExit(pathDst);
            files.append(pathDst.toUri().getPath());
          }
          first = false;
        }
      }
    }
    if (files.length() > 0) {
      // I've replaced the use of the missing setLocalFiles and
      // addLocalFiles methods (hadoop 0.23.x) with our own DistCacheUtils
      // methods which set the cache configurations directly.
      DistCacheUtils.addLocalFiles(conf, files.toString());
    }
  }

  /**
   * Add the Files to HDFS
   * 
   * @param conf The job's configuration
   * @param files Paths that should be transfered to HDFS
   */
  public static String addFilesToHDFS(Configuration conf, String files) {
    if (files == null)
      return null;
    String[] fileArr = files.split(",");
    String[] finalArr = new String[fileArr.length];

    for (int i = 0; i < fileArr.length; i++) {
      String tmp = fileArr[i];
      String finalPath;

      URI pathURI;
      try {
        pathURI = new URI(tmp);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }

      try {
        LocalFileSystem local = LocalFileSystem.getLocal(conf);
        Path pathSrc = new Path(pathURI);
        // LOG.info("pathSrc: " + pathSrc);

        if (local.exists(pathSrc)) {
          FileSystem hdfs = FileSystem.get(conf);
          Path pathDst = new Path(hdfs.getWorkingDirectory() + "/temp",
              pathSrc.getName());

          // LOG.info("WorkingDirectory: " + hdfs.getWorkingDirectory());
          LOG.debug("copyToHDFSFile: " + pathDst);
          hdfs.copyFromLocalFile(pathSrc, pathDst);
          hdfs.deleteOnExit(pathDst);

          finalPath = pathDst.makeQualified(hdfs).toString();
          finalArr[i] = finalPath;
        }
      } catch (IOException e) {
        LOG.error(e);
      }

    }
    return StringUtils.arrayToString(finalArr);
  }

  /**
   * Add the JARs from the given HDFS paths to the Classpath
   * 
   * @param conf The job's configuration
   */
  public static URL[] addJarsToJobClasspath(Configuration conf) {
    URL[] classLoaderURLs = ((URLClassLoader) conf.getClassLoader()).getURLs();
    String files = conf.get("tmpjars", "");

    if (!files.isEmpty()) {
      String[] fileArr = files.split(",");
      URL[] libjars = new URL[fileArr.length + classLoaderURLs.length];

      for (int i = 0; i < fileArr.length; i++) {
        String tmp = fileArr[i];

        URI pathURI;
        try {
          pathURI = new URI(tmp);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }

        try {
          FileSystem hdfs = FileSystem.get(conf);
          Path pathSrc = new Path(pathURI.getPath());
          // LOG.info("pathSrc: " + pathSrc);

          if (hdfs.exists(pathSrc)) {
            LocalFileSystem local = LocalFileSystem.getLocal(conf);

            // File dst = File.createTempFile(pathSrc.getName() + "-", ".jar");
            Path pathDst = new Path(local.getWorkingDirectory(),
                pathSrc.getName());

            LOG.debug("copyToLocalFile: " + pathDst);
            hdfs.copyToLocalFile(pathSrc, pathDst);
            local.deleteOnExit(pathDst);

            libjars[i] = new URL(local.makeQualified(pathDst).toString());
          }

        } catch (IOException ex) {
          throw new RuntimeException("Error setting up classpath", ex);
        }
      }

      // Add old classLoader entries
      int index = fileArr.length;
      for (int i = 0; i < classLoaderURLs.length; i++) {
        libjars[index] = classLoaderURLs[i];
        index++;
      }

      // Set classloader in current conf/thread
      conf.setClassLoader(new URLClassLoader(libjars, conf.getClassLoader()));

      Thread.currentThread().setContextClassLoader(
          new URLClassLoader(libjars, Thread.currentThread()
              .getContextClassLoader()));

      // URL[] urls = ((URLClassLoader) conf.getClassLoader()).getURLs();
      // for (URL u : urls)
      // LOG.info("newClassLoader: " + u.getPath());

      // Set tmpjars
      // hdfs to local path
      String jars = "";
      for (int i = 0; i < fileArr.length; i++) {
        URL url = libjars[i];
        if (jars.length() > 0) {
          jars += ",";
        }
        jars += url.toString();
      }
      conf.set("tmpjars", jars);

      return libjars;
    }
    return null;
  }

}
