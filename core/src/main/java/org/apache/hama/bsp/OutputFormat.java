package org.apache.hama.bsp;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;

public interface OutputFormat<K, V> {

  /**
   * Get the {@link RecordWriter} for the given job.
   * 
   * @param ignored
   * @param job configuration for the job whose output is being written.
   * @param name the unique name for this part of the output.
   * @return a {@link RecordWriter} to write the output for the job.
   * @throws IOException
   */
  RecordWriter<K, V> getRecordWriter(FileSystem ignored, BSPJob job, String name)
      throws IOException;

  /**
   * Check for validity of the output-specification for the job.
   * 
   * <p>
   * This is to validate the output specification for the job when it is a job
   * is submitted. Typically checks that it does not already exist, throwing an
   * exception when it already exists, so that output is not overwritten.
   * </p>
   * 
   * @param ignored
   * @param job job configuration.
   * @throws IOException when output should not be attempted
   */
  void checkOutputSpecs(FileSystem ignored, BSPJob job) throws IOException;
}
