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
package org.apache.hama.pipes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.StringTokenizer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.FileInputFormat;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.OutputFormat;
import org.apache.hama.bsp.Partitioner;
import org.apache.hama.pipes.util.DistributedCacheUtil;

import com.google.common.base.Joiner;

/**
 * The main entry point and job submitter. It may either be used as a command
 * line-based or API-based method to launch Pipes jobs.
 * 
 * Adapted from Hadoop Pipes.
 * 
 */
public class Submitter implements Tool {

  protected static final Log LOG = LogFactory.getLog(Submitter.class);
  private HamaConfiguration conf;

  public Submitter() {
    this.conf = new HamaConfiguration();
  }

  public Submitter(HamaConfiguration conf) {
    setConf(conf);
  }

  @Override
  public HamaConfiguration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = (HamaConfiguration) conf;
  }

  /**
   * Get the URI of the CPU application's executable.
   * 
   * @param conf
   * @return the URI where the application's executable is located
   */
  public static String getExecutable(Configuration conf) {
    return conf.get("hama.pipes.executable");
  }

  /**
   * Set the URI for the CPU application's executable. Normally this is a hdfs:
   * location.
   * 
   * @param conf
   * @param executable The URI of the application's executable.
   */
  public static void setExecutable(Configuration conf, String executable) {
    conf.set("hama.pipes.executable", executable);
  }

  /**
   * Set the configuration, if it doesn't already have a value for the given
   * key.
   * 
   * @param conf the configuration to modify
   * @param key the key to set
   * @param value the new "default" value to set
   */
  private static void setIfUnset(Configuration conf, String key, String value) {
    if (conf.get(key) == null) {
      conf.set(key, value);
    }
  }

  /**
   * Save away the user's original partitioner before we override it.
   * 
   * @param conf the configuration to modify
   * @param cls the user's partitioner class
   */
  static void setJavaPartitioner(Configuration conf, Class<?> cls) {
    conf.set("hama.pipes.partitioner", cls.getName());
  }

  /**
   * Get the user's original partitioner.
   * 
   * @param conf the configuration to look in
   * @return the class that the user submitted
   */
  @SuppressWarnings("rawtypes")
  static Class<? extends Partitioner> getJavaPartitioner(Configuration conf) {
    return conf.getClass("hama.pipes.partitioner", HashPartitioner.class,
        Partitioner.class);
  }

  /**
   * Does the user want to keep the command file for debugging? If this is true,
   * pipes will write a copy of the command data to a file in the task directory
   * named "downlink.data", which may be used to run the C++ program under the
   * debugger. You probably also want to set
   * JobConf.setKeepFailedTaskFiles(true) to keep the entire directory from
   * being deleted. To run using the data file, set the environment variable
   * "hadoop.pipes.command.file" to point to the file.
   * 
   * @param conf the configuration to check
   * @return will the framework save the command file?
   */
  public static boolean getKeepCommandFile(Configuration conf) {
    return conf.getBoolean("hama.pipes.command-file.keep", false);
  }

  /**
   * Set whether to keep the command file for debugging
   * 
   * @param conf the configuration to modify
   * @param keep the new value
   */
  public static void setKeepCommandFile(Configuration conf, boolean keep) {
    conf.setBoolean("hama.pipes.command-file.keep", keep);
  }

  /**
   * Submit a job to the cluster. All of the necessary modifications to the job
   * to run under pipes are made to the configuration.
   * 
   * @param job to submit to the cluster
   * @throws IOException
   */
  public static void runJob(BSPJob job) throws IOException {
    setupPipesJob(job);
    BSPJobClient.runJob(job);
  }

  private static void setupPipesJob(BSPJob job) throws IOException {
    job.setBspClass(PipesBSP.class);
    job.setJarByClass(PipesBSP.class);

    String textClassname = Text.class.getName();
    setIfUnset(job.getConfiguration(), "bsp.input.key.class", textClassname);
    setIfUnset(job.getConfiguration(), "bsp.input.value.class", textClassname);
    setIfUnset(job.getConfiguration(), "bsp.output.key.class", textClassname);
    setIfUnset(job.getConfiguration(), "bsp.output.value.class", textClassname);
    setIfUnset(job.getConfiguration(), Constants.MESSAGE_CLASS,
        BytesWritable.class.getName());

    setIfUnset(job.getConfiguration(), "bsp.job.name", "Hama Pipes Job");

    // DEBUG Output
    LOG.debug("BspClass: " + job.getBspClass().getName());
    // conf.setInputFormat(NLineInputFormat.class);
    LOG.debug("InputFormat: " + job.getInputFormat());
    LOG.debug("InputKeyClass: " + job.getInputKeyClass().getName());
    LOG.debug("InputValueClass: " + job.getInputValueClass().getName());
    LOG.debug("InputFormat: " + job.getOutputFormat());
    LOG.debug("OutputKeyClass: " + job.getOutputKeyClass().getName());
    LOG.debug("OutputValueClass: " + job.getOutputValueClass().getName());
    LOG.debug("MessageClass: " + job.get(Constants.MESSAGE_CLASS));

    LOG.debug("bsp.master.address: "
        + job.getConfiguration().get("bsp.master.address"));
    LOG.debug("bsp.local.tasks.maximum: "
        + job.getConfiguration().get("bsp.local.tasks.maximum"));
    LOG.debug("NumBspTask: " + job.getNumBspTask());
    LOG.debug("fs.default.name: "
        + job.getConfiguration().get("fs.default.name"));

    String exec = getExecutable(job.getConfiguration());
    if (exec == null) {
      throw new IllegalArgumentException(
          "No application defined. (Set property hama.pipes.executable)");
    }

    URI[] fileCache = DistributedCache.getCacheFiles(job.getConfiguration());
    if (fileCache == null) {
      fileCache = new URI[1];
    } else {
      URI[] tmp = new URI[fileCache.length + 1];
      System.arraycopy(fileCache, 0, tmp, 1, fileCache.length);
      fileCache = tmp;
    }

    try {
      fileCache[0] = new URI(exec);
    } catch (URISyntaxException e) {
      IOException ie = new IOException("Problem parsing executable URI " + exec);
      ie.initCause(e);
      throw ie;
    }
    DistributedCache.setCacheFiles(fileCache, job.getConfiguration());

    // Add libjars to HDFS
    String tmpjars = job.getConfiguration().get("tmpjars");
    LOG.debug("conf.get(tmpjars): " + tmpjars);

    if (tmpjars != null) {
      String hdfsFileUrls = DistributedCacheUtil.addFilesToHDFS(
          job.getConfiguration(), job.getConfiguration().get("tmpjars"));
      job.getConfiguration().set("tmpjars", hdfsFileUrls);

      LOG.info("conf.get(tmpjars): " + job.getConfiguration().get("tmpjars"));
    }

  }

  /**
   * A command line parser for the CLI-based Pipes job submitter.
   */
  static class CommandLineParser {
    private Options options = new Options();

    void addOption(String longName, boolean required, String description,
        String paramName) {
      OptionBuilder.withArgName(paramName);
      OptionBuilder.hasArgs(1);
      OptionBuilder.withDescription(description);
      OptionBuilder.isRequired(required);
      Option option = OptionBuilder.create(longName);
      options.addOption(option);
    }

    void addArgument(String name, boolean required, String description) {
      OptionBuilder.withArgName(name);
      OptionBuilder.hasArgs(1);
      OptionBuilder.withDescription(description);
      OptionBuilder.isRequired(required);
      Option option = OptionBuilder.create();
      options.addOption(option);

    }

    Parser createParser() {
      Parser result = new BasicParser();
      return result;
    }

    void printUsage() {
      // The CLI package should do this for us, but I can't figure out how
      // to make it print something reasonable.
      System.out.println("bin/hama pipes [genericOptions] [pipesOptions]");
      System.out.println();

      // Generic options
      // GenericOptionsParser.printGenericCommandUsage(System.out);
      System.out.println("Generic options supported are");
      System.out
          .println(" -conf <configuration file>\tSpecify an application configuration file");
      System.out.println(" -D <property=value>\tUse value for given property");
      System.out.println();

      // Pipes options
      System.out.println("Pipes options supported are");
      System.out.println(" -input <path>\tInput directory");
      System.out.println(" -output <path>\tOutput directory");
      System.out.println(" -jar <jar file>\tJar filename");
      System.out.println(" -inputformat <class>\tInputFormat class");
      System.out.println(" -bspTasks <number>\tNumber of bsp tasks to launch");
      System.out.println(" -partitioner <class>\tJava Partitioner");
      System.out.println(" -combiner <class>\tJava Combiner class");
      System.out.println(" -outputformat <class>\tJava RecordWriter");
      System.out
          .println(" -cachefiles <space separated paths>\tAdditional cache files like libs, can be globbed with wildcards");
      System.out.println(" -program <executable>\tExecutable URI");
      System.out.println(" -programArgs <argument>\tArguments for the program");
      System.out
          .println(" -interpreter <executable>\tInterpreter, like python or bash");
      System.out
          .println(" -streaming <true|false>\tIf supplied, streaming is used instead of pipes");
      System.out.println(" -jobname <name>\tSets the name of this job");
    }
  }

  private static <InterfaceType> Class<? extends InterfaceType> getClass(
      CommandLine cl, String key, HamaConfiguration conf,
      Class<InterfaceType> cls) throws ClassNotFoundException {

    return conf.getClassByName(cl.getOptionValue(key)).asSubclass(cls);
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLineParser cli = new CommandLineParser();
    if (args.length == 0) {
      cli.printUsage();
      return 1;
    }

    LOG.debug("Hama pipes Submitter started!");

    cli.addOption("input", false, "input path for bsp", "path");
    cli.addOption("output", false, "output path from bsp", "path");

    cli.addOption("jar", false, "job jar file", "path");
    cli.addOption("inputformat", false, "java classname of InputFormat",
        "class");
    // cli.addArgument("javareader", false, "is the RecordReader in Java");

    cli.addOption("partitioner", false, "java classname of Partitioner",
        "class");
    cli.addOption("outputformat", false, "java classname of OutputFormat",
        "class");

    cli.addOption("cachefiles", false, "additional cache files to add",
        "space delimited paths");

    cli.addOption("interpreter", false, "interpreter, like python or bash",
        "executable");

    cli.addOption("jobname", false, "the jobname", "name");

    cli.addOption("programArgs", false, "program arguments", "arguments");
    cli.addOption("bspTasks", false, "how many bsp tasks to launch", "number");
    cli.addOption("streaming", false,
        "if supplied, streaming is used instead of pipes", "");

    cli.addOption(
        "jobconf",
        false,
        "\"n1=v1,n2=v2,..\" (Deprecated) Optional. Add or override a JobConf property.",
        "key=val");

    cli.addOption("program", false, "URI to application executable", "class");
    Parser parser = cli.createParser();
    try {

      // check generic arguments -conf
      GenericOptionsParser genericParser = new GenericOptionsParser(getConf(),
          args);
      // get other arguments
      CommandLine results = parser.parse(cli.options,
          genericParser.getRemainingArgs());

      BSPJob job = new BSPJob(getConf());

      if (results.hasOption("input")) {
        FileInputFormat.setInputPaths(job, results.getOptionValue("input"));
      }
      if (results.hasOption("output")) {
        FileOutputFormat.setOutputPath(job,
            new Path(results.getOptionValue("output")));
      }
      if (results.hasOption("jar")) {
        job.setJar(results.getOptionValue("jar"));
      }

      if (results.hasOption("jobname")) {
        job.setJobName(results.getOptionValue("jobname"));
      }

      if (results.hasOption("inputformat")) {
        job.setInputFormat(getClass(results, "inputformat", conf,
            InputFormat.class));
      }

      if (results.hasOption("partitioner")) {
        job.setPartitioner(getClass(results, "partitioner", conf,
            Partitioner.class));
      }

      if (results.hasOption("outputformat")) {
        job.setOutputFormat(getClass(results, "outputformat", conf,
            OutputFormat.class));
      }

      if (results.hasOption("streaming")) {
        LOG.info("Streaming enabled!");
        job.set("hama.streaming.enabled", "true");
      }

      if (results.hasOption("jobconf")) {
        LOG.warn("-jobconf option is deprecated, please use -D instead.");
        String options = results.getOptionValue("jobconf");
        StringTokenizer tokenizer = new StringTokenizer(options, ",");
        while (tokenizer.hasMoreTokens()) {
          String keyVal = tokenizer.nextToken().trim();
          String[] keyValSplit = keyVal.split("=", 2);
          job.set(keyValSplit[0], keyValSplit[1]);
        }
      }

      if (results.hasOption("bspTasks")) {
        int optionValue = Integer.parseInt(results.getOptionValue("bspTasks"));
        conf.setInt("bsp.local.tasks.maximum", optionValue);
        conf.setInt("bsp.peers.num", optionValue);
      }

      if (results.hasOption("program")) {
        String executablePath = results.getOptionValue("program");
        setExecutable(job.getConfiguration(), executablePath);
        DistributedCache.addCacheFile(new Path(executablePath).toUri(), conf);
      }

      if (results.hasOption("interpreter")) {
        job.getConfiguration().set("hama.pipes.executable.interpretor",
            results.getOptionValue("interpreter"));
      }

      if (results.hasOption("programArgs")) {
        job.getConfiguration().set("hama.pipes.executable.args",
            Joiner.on(" ").join(results.getOptionValues("programArgs")));
        // job.getConfiguration().set("hama.pipes.resolve.executable.args",
        // "true");
      }

      if (results.hasOption("cachefiles")) {
        FileSystem fs = FileSystem.get(getConf());
        String[] optionValues = results.getOptionValues("cachefiles");
        for (String s : optionValues) {
          Path path = new Path(s);
          FileStatus[] globStatus = fs.globStatus(path);
          for (FileStatus f : globStatus) {
            if (!f.isDir()) {
              DistributedCache.addCacheFile(f.getPath().toUri(),
                  job.getConfiguration());
            } else {
              LOG.info("Ignoring directory " + f.getPath() + " while globbing.");
            }
          }
        }
      }

      // if they gave us a jar file, include it into the class path
      String jarFile = job.getJar();
      if (jarFile != null) {
        @SuppressWarnings("deprecation")
        final URL[] urls = new URL[] { FileSystem.getLocal(conf)
            .pathToFile(new Path(jarFile)).toURL() };
        // FindBugs complains that creating a URLClassLoader should be
        // in a doPrivileged() block.
        ClassLoader loader = AccessController
            .doPrivileged(new PrivilegedAction<ClassLoader>() {
              @Override
              public ClassLoader run() {
                return new URLClassLoader(urls);
              }
            });
        conf.setClassLoader(loader);
      }

      runJob(job);
      return 0;
    } catch (ParseException pe) {
      LOG.info("Error : " + pe);
      cli.printUsage();
      return 1;
    }

  }

  /**
   * Submit a pipes job based on the command line arguments.
   */
  public static void main(String[] args) throws Exception {
    int exitCode = new Submitter().run(args);
    System.exit(exitCode);
  }

}
