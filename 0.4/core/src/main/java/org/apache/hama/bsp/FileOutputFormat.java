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

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;

public abstract class FileOutputFormat<K, V> implements OutputFormat<K, V> {

  /**
   * Set whether the output of the job is compressed.
   * 
   * @param conf the {@link JobConf} to modify
   * @param compress should the output of the job be compressed?
   */
  public static void setCompressOutput(BSPJob conf, boolean compress) {
    conf.getConf().setBoolean("bsp.output.compress", compress);
  }

  /**
   * Is the job output compressed?
   * 
   * @param conf the {@link JobConf} to look in
   * @return <code>true</code> if the job output should be compressed,
   *         <code>false</code> otherwise
   */
  public static boolean getCompressOutput(BSPJob conf) {
    return conf.getConf().getBoolean("bsp.output.compress", false);
  }

  /**
   * Set the {@link CompressionCodec} to be used to compress job outputs.
   * 
   * @param conf the {@link JobConf} to modify
   * @param codecClass the {@link CompressionCodec} to be used to compress the
   *          job outputs
   */
  public static void setOutputCompressorClass(BSPJob conf,
      Class<? extends CompressionCodec> codecClass) {
    setCompressOutput(conf, true);
    conf.getConf().setClass("bsp.output.compression.codec", codecClass,
        CompressionCodec.class);
  }

  /**
   * Get the {@link CompressionCodec} for compressing the job outputs.
   * 
   * @param conf the {@link JobConf} to look in
   * @param defaultValue the {@link CompressionCodec} to return if not set
   * @return the {@link CompressionCodec} to be used to compress the job outputs
   * @throws IllegalArgumentException if the class was specified, but not found
   */
  public static Class<? extends CompressionCodec> getOutputCompressorClass(
      BSPJob conf, Class<? extends CompressionCodec> defaultValue) {
    Class<? extends CompressionCodec> codecClass = defaultValue;

    String name = conf.get("bsp.output.compression.codec");
    if (name != null) {
      try {
        codecClass = conf.getConf().getClassByName(name).asSubclass(
            CompressionCodec.class);
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException("Compression codec " + name
            + " was not found.", e);
      }
    }
    return codecClass;
  }

  public abstract RecordWriter<K, V> getRecordWriter(FileSystem ignored,
      BSPJob job, String name) throws IOException;

  public void checkOutputSpecs(FileSystem ignored, BSPJob job)
      throws FileAlreadyExistsException, InvalidJobConfException, IOException {
    // Ensure that the output directory is set and not already there
    Path outDir = getOutputPath(job);
    if (outDir == null && job.getNumBspTask() != 0) {
      throw new InvalidJobConfException("Output directory not set in JobConf.");
    }
    if (outDir != null) {
      FileSystem fs = outDir.getFileSystem(job.getConf());
      // normalize the output directory
      outDir = fs.makeQualified(outDir);
      setOutputPath(job, outDir);
      // check its existence
      if (fs.exists(outDir)) {
        throw new FileAlreadyExistsException("Output directory " + outDir
            + " already exists");
      }
    }
  }

  /**
   * Set the {@link Path} of the output directory for the map-reduce job.
   * 
   * @param conf The configuration of the job.
   * @param outputDir the {@link Path} of the output directory for the
   *          map-reduce job.
   */
  public static void setOutputPath(BSPJob conf, Path outputDir) {
    outputDir = new Path(conf.getWorkingDirectory(), outputDir);
    conf.set("bsp.output.dir", outputDir.toString());
  }

  /**
   * Set the {@link Path} of the task's temporary output directory for the
   * map-reduce job.
   * 
   * <p>
   * <i>Note</i>: Task output path is set by the framework.
   * </p>
   * 
   * @param conf The configuration of the job.
   * @param outputDir the {@link Path} of the output directory for the
   *          map-reduce job.
   */

  static void setWorkOutputPath(BSPJob conf, Path outputDir) {
    outputDir = new Path(conf.getWorkingDirectory(), outputDir);
    conf.set("bsp.work.output.dir", outputDir.toString());
  }

  /**
   * Get the {@link Path} to the output directory for the map-reduce job.
   * 
   * @return the {@link Path} to the output directory for the map-reduce job.
   * @see FileOutputFormat#getWorkOutputPath(JobConf)
   */
  public static Path getOutputPath(BSPJob conf) {
    String name = conf.get("bsp.output.dir");
    return name == null ? null : new Path(name);
  }

  public static Path getWorkOutputPath(BSPJob conf) {
    String name = conf.get("bsp.work.output.dir");
    return name == null ? null : new Path(name);
  }

  /**
   * Helper function to create the task's temporary output directory and return
   * the path to the task's output file.
   * 
   * @param conf job-configuration
   * @param name temporary task-output filename
   * @return path to the task's temporary output file
   * @throws IOException
   */
  public static Path getTaskOutputPath(BSPJob conf, String name)
      throws IOException {
    // ${bsp.out.dir}
    Path outputPath = getOutputPath(conf);
    if (outputPath == null) {
      throw new IOException("Undefined job output-path");
    }

    Path workPath = outputPath;

    // ${bsp.out.dir}/_temporary/_${taskid}/${name}
    return new Path(workPath, name);
  }

  /**
   * Helper function to generate a name that is unique for the task.
   * 
   * @param conf the configuration for the job.
   * @param name the name to make unique.
   * @return a unique name accross all tasks of the job.
   */
  public static String getUniqueName(BSPJob conf, String name) {
    int partition = conf.getInt("bsp.task.partition", -1);
    if (partition == -1) {
      throw new IllegalArgumentException(
          "This method can only be called from within a Job");
    }

    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(5);
    numberFormat.setGroupingUsed(false);

    return name + "-" + numberFormat.format(partition);
  }

  /**
   * Helper function to generate a {@link Path} for a file that is unique for
   * the task within the job output directory.
   * 
   * @param conf the configuration for the job.
   * @param name the name for the file.
   * @return a unique path accross all tasks of the job.
   */
  public static Path getPathForCustomFile(BSPJob conf, String name) {
    return new Path(getWorkOutputPath(conf), getUniqueName(conf, name));
  }
}
