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
package org.apache.hama.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.junit.Test;

import junit.framework.TestCase;

public class BipartiteMatchingTest extends TestCase{

  private String[] input = {
      "A L:B D",
      "B R:A C",
      "C L:B D",
      "D R:A C"
  };

  private final static String DELIMETER = "\t";

  @SuppressWarnings("serial")
  private Map<String, String> output1 = new HashMap<String, String>()
  {{
    put("C", "TextPair{MatchVertex=D, Component=L}");
    put("A", "TextPair{MatchVertex=B, Component=L}");
    put("D", "TextPair{MatchVertex=C, Component=R}");
    put("B", "TextPair{MatchVertex=A, Component=R}");
  }};


  private static String INPUT = "/tmp/graph.txt";
  private static String OUTPUT = "/tmp/graph-bipartite";

  private Configuration conf = new HamaConfiguration();
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    fs = FileSystem.get(conf);
  }

  private void generateTestData(){
    FileWriter fout = null;
    BufferedWriter bout = null;
    PrintWriter pout = null;
    try{
      fout = new FileWriter(INPUT);
      bout = new BufferedWriter(fout);
      pout = new PrintWriter(bout);
      for(String line:input){
        pout.println(line);
      }
    }
    catch(IOException e){
      e.printStackTrace();
    }
    finally{      
      try {
        if(pout!=null){pout.close();}
        if(bout!=null){bout.close();}
        if(fout!=null){fout.close();}
      } catch (IOException e) {
        e.printStackTrace();
      }      
    }
  }


  private void verifyResult()throws IOException{
    FileStatus[] files = fs.globStatus(new Path(OUTPUT + "/part-*"));
    Text key = new Text();
    Text value = new Text();
    for(FileStatus file:files){
      if(file.getLen() > 0){
        FSDataInputStream in = fs.open(file.getPath());        
        BufferedReader bin = new BufferedReader(
            new InputStreamReader(in));

        String s = bin.readLine();
        while(s!=null){
          next(key, value, s);
          String expValue = output1.get(key.toString());
          assertEquals(expValue, value.toString());
          System.out.println(key + " "+value);
          s = bin.readLine();
        }        
        in.close();
      }
    }
  }

  private static void next(Text key, Text value, String line){
    String[] lineA = line.split(DELIMETER);
    key.set(lineA[0]);
    value.set(lineA[1]);
  }

  private void deleteTempDirs() {
    try {
      if (fs.exists(new Path(INPUT)))
        fs.delete(new Path(INPUT), true);
      if (fs.exists(new Path(OUTPUT)))
        fs.delete(new Path(OUTPUT), true);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testBipartiteMatching() throws IOException, InterruptedException,
  ClassNotFoundException{    
    generateTestData();
    try {
      String seed = "2";
      BipartiteMatching.main(new String[] { INPUT, OUTPUT, "30", "2",
          seed});
      verifyResult();
    } finally {
      deleteTempDirs();
    }
  }


}
