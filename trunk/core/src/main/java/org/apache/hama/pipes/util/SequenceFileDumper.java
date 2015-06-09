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

import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hama.HamaConfiguration;

public class SequenceFileDumper {

  protected static final Log LOG = LogFactory.getLog(SequenceFileDumper.class);
  public static String LINE_SEP = System.getProperty("line.separator");

  private SequenceFileDumper() {
  }

  /**
   * A command line parser for the CLI-based SequenceFileDumper.
   */
  static class CommandLineParser {
    private Options options = new Options();

    void addOption(String longName, boolean required, String description,
        String paramName) {
      Option option = OptionBuilder.withArgName(paramName).hasArgs(1)
          .withDescription(description).isRequired(required).create(longName);
      options.addOption(option);
    }

    void addArgument(String name, boolean required, String description) {
      Option option = OptionBuilder.withArgName(name).hasArgs(1)
          .withDescription(description).isRequired(required).create();
      options.addOption(option);
    }

    Parser createParser() {
      return new BasicParser();
    }

    void printUsage() {
      // The CLI package should do this for us, but I can't figure out how
      // to make it print something reasonable.
      System.out.println("hama seqdumper");
      System.out.println("  [-file <path>] // The sequence file to read");
      System.out
          .println("  [-output <path>] // The output file. If not specified, dumps to the console");
      System.out
          .println("  [-substring <number> // The number of chars of value to print");
      System.out.println("  [-count <true>] // Report the count only");
      System.out.println();
    }
  }

  public static void main(String[] args) throws Exception {
    CommandLineParser cli = new CommandLineParser();
    if (args.length == 0) {
      cli.printUsage();
      return;
    }

    // Add arguments
    cli.addOption("file", false, "The Sequence File containing the Clusters",
        "path");
    cli.addOption("output", false,
        "The output file.  If not specified, dumps to the console", "path");
    cli.addOption("substring", false,
        "The number of chars of the FormatString() to print", "number");
    cli.addOption("count", false, "Report the count only", "number");

    Parser parser = cli.createParser();
    try {
      HamaConfiguration conf = new HamaConfiguration();
      CommandLine cmdLine = parser.parse(cli.options, args);

      if (cmdLine.hasOption("file")) {
        Path path = new Path(cmdLine.getOptionValue("file"));

        FileSystem fs = FileSystem.get(path.toUri(), conf);
        if (!fs.isFile(path)) {
          System.out.println("File does not exist: " + path.toString());
          return;
        }
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);

        Writer writer;
        if (cmdLine.hasOption("output")) {
          writer = new FileWriter(cmdLine.getOptionValue("output"));
        } else {
          writer = new OutputStreamWriter(System.out);
        }

        writer.append("Input Path: ").append(String.valueOf(path))
            .append(LINE_SEP);

        int sub = Integer.MAX_VALUE;
        if (cmdLine.hasOption("substring")) {
          sub = Integer.parseInt(cmdLine.getOptionValue("substring"));
        }

        Writable key;
        if (reader.getKeyClass() != NullWritable.class) {
          key = (Writable) reader.getKeyClass().newInstance();
        } else {
          key = NullWritable.get();
        }
        Writable value;
        if (reader.getValueClass() != NullWritable.class) {
          value = (Writable) reader.getValueClass().newInstance();
        } else {
          value = NullWritable.get();
        }

        writer.append("Key class: ")
            .append(String.valueOf(reader.getKeyClass()))
            .append(" Value Class: ").append(String.valueOf(value.getClass()))
            .append(LINE_SEP);
        writer.flush();

        long count = 0;
        boolean countOnly = cmdLine.hasOption("count");
        if (countOnly == false) {
          while (reader.next(key, value)) {
            writer.append("Key: ").append(String.valueOf(key));
            String str = value.toString();
            writer.append(": Value: ").append(
                str.length() > sub ? str.substring(0, sub) : str);
            writer.write(LINE_SEP);
            writer.flush();
            count++;
          }
          writer.append("Count: ").append(String.valueOf(count))
              .append(LINE_SEP);

        } else { // count only
          while (reader.next(key, value)) {
            count++;
          }
          writer.append("Count: ").append(String.valueOf(count))
              .append(LINE_SEP);
        }
        writer.flush();

        if (cmdLine.hasOption("output")) {
          writer.close();
        }
        reader.close();

      } else {
        cli.printUsage();
      }

    } catch (ParseException e) {
      LOG.error(e.getMessage());
      cli.printUsage();
      return;
    }
  }
}
