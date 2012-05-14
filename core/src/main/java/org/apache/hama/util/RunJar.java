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
package org.apache.hama.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.apache.hadoop.fs.FileUtil;

/**
 * Run a Hama job jar.
 */
public class RunJar {

  /** Unpack a jar file into a directory. */
  public static void unJar(File jarFile, File toDir) throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = (JarEntry) entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            file.getParentFile().mkdirs();
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      jar.close();
    }
  }

  /**
   * Run a Hama job jar. If the main class is not in the jar's manifest, then
   * it must be provided on the command line.
   */
  public static void main(String[] args) throws Throwable {
    String usage = "Usage: hama jar <jar> [mainClass] args...";

    if (args.length < 1) {
      System.err.println(usage);
      System.exit(-1);
    }

    int firstArg = 0;
    String fileName = args[firstArg++];
    File file = new File(fileName);
    String mainClassName = null;

    JarFile jarFile = new JarFile(fileName);
    Manifest manifest = jarFile.getManifest();
    if (manifest != null) {
      mainClassName = manifest.getMainAttributes().getValue("Main-Class");
    }
    jarFile.close();

    if (mainClassName == null) {
      if (args.length < 2) {
        System.err.println(usage);
        System.exit(-1);
      }
      mainClassName = args[firstArg++];
    }
    mainClassName = mainClassName.replaceAll("/", ".");

    final File workDir = File.createTempFile("hama-unjar", "");
    workDir.delete();
    workDir.mkdirs();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        try {
          FileUtil.fullyDelete(workDir);
        } catch (IOException e) {
        }
      }
    });

    unJar(file, workDir);

    List<URL> classPath = new ArrayList<URL>();
    classPath.add(new File(workDir + "/").toURI().toURL());
    classPath.add(file.toURI().toURL());
    classPath.add(new File(workDir, "classes/").toURI().toURL());
    File[] libs = new File(workDir, "lib").listFiles();
    if (libs != null) {
      for (File lib : libs) {
        classPath.add(lib.toURI().toURL());
      }
    }
    ClassLoader loader = new URLClassLoader((URL[]) classPath
      .toArray(new URL[classPath.size()]));

    Thread.currentThread().setContextClassLoader(loader);
    Class<?> mainClass = loader.loadClass(mainClassName);
    Method main = mainClass.getMethod("main", new Class[] { Array.newInstance(
        String.class, 0).getClass() });
    List<String> var = Arrays.asList(args).subList(firstArg,
      args.length);
    String[] newArgs = (String[]) var.toArray(new String[var.size()]);
    try {
      main.invoke(null, new Object[] { newArgs });
    } catch (InvocationTargetException e) {
      throw e.getTargetException();
    }
  }

}
