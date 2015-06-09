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
package org.apache.hama.ml.recommendation.cf;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.SequenceFileInputFormat;
import org.apache.hama.bsp.SequenceFileOutputFormat;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DoubleMatrix;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.commons.math.SquareVectorFunction;
import org.apache.hama.ml.recommendation.ItemSimilarity;
import org.apache.hama.ml.recommendation.Preference;
import org.apache.hama.ml.recommendation.Recommender;
import org.apache.hama.ml.recommendation.RecommenderIO;
import org.apache.hama.ml.recommendation.UserSimilarity;
import org.apache.hama.ml.recommendation.cf.function.MeanAbsError;
import org.apache.hama.ml.recommendation.cf.function.OnlineUpdate;
import org.apache.hama.ml.recommendation.cf.function.OnlineUpdate.Function;
import org.apache.hama.ml.recommendation.cf.function.OnlineUpdate.InputStructure;

public class OnlineCF implements Recommender, RecommenderIO, UserSimilarity, ItemSimilarity{

  public static class Settings {
    
    // configuration strings
    // delimiters for input data
    public static final String CONF_INPUT_PREFERENCES_DELIM = "ml.recommender.cf.input.delim.preferences";
    public static final String CONF_INPUT_USER_DELIM = "ml.recommender.cf.input.delim.user.features";
    public static final String CONF_INPUT_ITEM_DELIM = "ml.recommender.cf.input.delim.item.features";
    // delimiters for output data (trained model)
    public static final String CONF_MODEL_USER_DELIM = "ml.recommender.cf.model.delim.user";
    public static final String CONF_MODEL_ITEM_DELIM = "ml.recommender.cf.model.delim.item";
    public static final String CONF_MODEL_USER_FEATURE_DELIM = "ml.recommender.cf.model.delim.user.features";
    public static final String CONF_MODEL_ITEM_FEATURE_DELIM = "ml.recommender.cf.model.delim.item.features";

    public static final String CONF_ITERATION_COUNT = "ml.recommender.cf.iterations";
    public static final String CONF_MATRIX_RANK = "ml.recommender.cf.rank";
    public static final String CONF_TASK_COUNT = "ml.recommender.cf.task.count";
    public static final String CONF_SKIP_COUNT = "ml.recommender.cf.skip.count";

    public static final String CONF_ONLINE_UPDATE_FUNCTION = "ml.recommender.cf.func.ou";
  
    // Message types
    public static final IntWritable MSG_INP_USER_FEATURES = new IntWritable(0);
    public static final IntWritable MSG_INP_ITEM_FEATURES = new IntWritable(1);
    public static final IntWritable MSG_ITEM_MATRIX = new IntWritable(2);
    public static final IntWritable MSG_ITEM_FEATURE_MATRIX = new IntWritable(3);
    public static final IntWritable MSG_USER_FEATURE_MATRIX = new IntWritable(4);
    public static final IntWritable MSG_SENDER_ID = new IntWritable(5);
    public static final IntWritable MSG_VALUE = new IntWritable(6);
    
    // TODO: currently we support only one input
    //     if multiple inputs support will be added
    //     change inputPath accordingly
    public static final String CONF_INPUT_PATH = "ml.recommender.cf.input.path";
    public static final String CONF_OUTPUT_PATH = "ml.recommender.cf.output.path";

    // default values
    public static final int DFLT_ITERATION_COUNT = 100;
    public static final int DFLT_MATRIX_RANK = 10;
    public static final int DFLT_SKIP_COUNT = 5;
    
    // used for delimiting input data and we assume they will be length of one
    public static final String DFLT_PREFERENCE_DELIM = "p";
    public static final String DFLT_USER_DELIM = "u";
    public static final String DFLT_ITEM_DELIM = "i";
  
    //used for delimiting output data (trained model)
    public static final String DFLT_MODEL_USER_DELIM = "a";
    public static final String DFLT_MODEL_ITEM_DELIM = "b";
    //since user feature models are matrices,
    //value is in form of (matrix_rank, matrix_converted_to_vector)
    //and they don't have id of key, in order to avoid crash
    //while parsing values, put some unnecessary value
    public static final String DFLT_MODEL_USER_MTX_FEATURES_DELIM = "c";
    public static final String DFLT_MODEL_ITEM_MTX_FEATURES_DELIM = "d";
    
    public static final String DFLT_MODEL_USER_FEATURES_DELIM = "e";
    public static final String DFLT_MODEL_ITEM_FEATURES_DELIM = "f";
  
    public static final Class<? extends OnlineUpdate.Function> DFLT_UPDATE_FUNCTION = MeanAbsError.class;

  } // Settings
  
  protected static Log LOG = LogFactory.getLog(OnlineCF.class);
  HamaConfiguration conf = new HamaConfiguration();
  // used only if model is loaded in memory
  private HashMap<Long, VectorWritable> modelUserFactorizedValues = new HashMap<Long, VectorWritable>();
  private HashMap<Long, VectorWritable> modelItemFactorizedValues = new HashMap<Long, VectorWritable>();
  private HashMap<Long, VectorWritable> modelUserFeatures = new HashMap<Long, VectorWritable>();
  private HashMap<Long, VectorWritable> modelItemFeatures = new HashMap<Long, VectorWritable>();
  private DoubleMatrix modelUserFeatureFactorizedValues = null;
  private DoubleMatrix modelItemFeatureFactorizedValues = null;
  private String modelPath = null;
  private boolean isLazyLoadModel = false;
  private Function function = null;
  
  /**
   * iteration count for matrix factorization
   * @param count - iteration count
   */
  public void setIteration(int count) {
    conf.setInt(OnlineCF.Settings.CONF_ITERATION_COUNT, count);
  }
  
  /**
   * Setting matrix rank for factorization
   * @param rank - matrix rank
   */
  public void setMatrixRank(int rank) {
    conf.setInt(OnlineCF.Settings.CONF_MATRIX_RANK, rank);
  }
  
  /**
   * Setting task count
   * @param count - task count
   */
  public void setTaskCount(int count) {
    conf.setInt(OnlineCF.Settings.CONF_TASK_COUNT, count);
  }
  
  /**
   * Online CF needs normalization of values
   * this configuration is set after how many iteration
   * of calculation values should be normalized between
   * different items
   * @param count - skip count before doing convergence
   */
  public void setSkipCount(int count) {
    conf.setInt(OnlineCF.Settings.CONF_SKIP_COUNT, count);
  }
  
  @Override
  public void setInputPreferences(String path) {
    LOG.debug("path = " + path);
    String alreadySetPath = conf.get(OnlineCF.Settings.CONF_INPUT_PATH, null);
    if (alreadySetPath != null && !alreadySetPath.equals(path)) {
      throw new InputMismatchException("different input path given" 
                                    + ", old: " + alreadySetPath
                                    + ", current:" + path);
    }
    conf.set(OnlineCF.Settings.CONF_INPUT_PATH, path);
  }

  @Override
  public void setInputUserFeatures(String path) {
    LOG.debug("path = " + path);
    String alreadySetPath = conf.get(OnlineCF.Settings.CONF_INPUT_PATH, null);
    if (alreadySetPath != null && !alreadySetPath.equals(path)) {
      throw new InputMismatchException("different input path given" 
                                    + ", old: " + alreadySetPath
                                    + ", current:" + path);
    }
    conf.set(OnlineCF.Settings.CONF_INPUT_PATH, path);
  }

  @Override
  public void setInputItemFeatures(String path) {
    LOG.debug("path = " + path);
    String alreadySetPath = conf.get(OnlineCF.Settings.CONF_INPUT_PATH, null);
    if (alreadySetPath != null && !alreadySetPath.equals(path)) {
      throw new InputMismatchException("different input path given" 
                                    + ", old: " + alreadySetPath
                                    + ", current:" + path);
    }
    conf.set(OnlineCF.Settings.CONF_INPUT_PATH, path);    
  }

  @Override
  public void setOutputPath(String path) {
    conf.set(OnlineCF.Settings.CONF_OUTPUT_PATH, path);
  }

  /**
   * Set update function to be used in compute phase
   * of online cf train bsp
   * @param cls
   */
  public void setUpdateFunction(Class<? extends OnlineUpdate.Function> cls) {
    conf.setClass(OnlineCF.Settings.CONF_ONLINE_UPDATE_FUNCTION, cls, OnlineUpdate.Function.class);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean train() {
    try {
      BSPJob job = setupJob();
      boolean res = job.waitForCompletion(true);
      return res;
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return false;
  }

  private BSPJob setupJob() throws IOException {
    BSPJob job = new BSPJob(conf, OnlineCF.class);

    String input = conf.get(OnlineCF.Settings.CONF_INPUT_PATH, null);
    String output = conf.get(OnlineCF.Settings.CONF_OUTPUT_PATH, null);
    Path in = new Path(input);
    Path out = new Path(output);

    if (conf.getInt(OnlineCF.Settings.CONF_MATRIX_RANK, -1) == -1) {
      conf.setInt(OnlineCF.Settings.CONF_MATRIX_RANK, OnlineCF.Settings.DFLT_MATRIX_RANK);
    }
    
    if (conf.getInt(OnlineCF.Settings.CONF_ITERATION_COUNT, -1) == -1) {
      conf.setInt(OnlineCF.Settings.CONF_ITERATION_COUNT, OnlineCF.Settings.DFLT_ITERATION_COUNT);
    }

    if (conf.getInt(OnlineCF.Settings.CONF_SKIP_COUNT, -1) == -1) {
      conf.setInt(OnlineCF.Settings.CONF_SKIP_COUNT, OnlineCF.Settings.DFLT_SKIP_COUNT);
    }
    
    if (conf.getClass(OnlineCF.Settings.CONF_ONLINE_UPDATE_FUNCTION, null) == null) {
      conf.setClass(OnlineCF.Settings.CONF_ONLINE_UPDATE_FUNCTION, OnlineCF.Settings.DFLT_UPDATE_FUNCTION,
              OnlineUpdate.Function.class);
    }
    conf.set(OnlineCF.Settings.CONF_MODEL_USER_DELIM, OnlineCF.Settings.DFLT_MODEL_USER_DELIM);
    conf.set(OnlineCF.Settings.CONF_MODEL_USER_FEATURE_DELIM, OnlineCF.Settings.DFLT_MODEL_USER_MTX_FEATURES_DELIM);
    conf.set(OnlineCF.Settings.CONF_MODEL_ITEM_DELIM, OnlineCF.Settings.DFLT_MODEL_ITEM_DELIM);
    conf.set(OnlineCF.Settings.CONF_MODEL_ITEM_FEATURE_DELIM, OnlineCF.Settings.DFLT_MODEL_ITEM_MTX_FEATURES_DELIM);

    job.setJobName("Online CF");
    job.setBoolean(Constants.ENABLE_RUNTIME_PARTITIONING, true);
    job.setPartitioner(HashPartitioner.class);
    job.setBspClass(OnlineTrainBSP.class);

    job.setInputPath(in);
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setInputKeyClass(Text.class);
    job.setInputValueClass(VectorWritable.class);

    job.setOutputPath(out);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(VectorWritable.class);

    job.setNumBspTask(conf.getInt(OnlineCF.Settings.CONF_TASK_COUNT, job.getNumBspTask()));
    return job;
  }

  @Override
  public boolean save() {
    // default behaivor is saving after training, 
    // we cannot hold model in memory after bsp
    return true;
  }

  @Override
  public boolean load(String path, boolean lazy) {
    this.isLazyLoadModel = lazy;
    this.modelPath = path;
    if (lazy == false) {
      Configuration conf = new Configuration();
      Path dataPath = new Path(modelPath);

      try {
        FileSystem fs = dataPath.getFileSystem(conf);
        LinkedList<Path> files = new LinkedList<Path>();

        if (!fs.exists(dataPath)) {
          this.isLazyLoadModel = false;
          this.modelPath = null;
          return false;
        }
        
        if(!fs.isFile(dataPath)) {
          for (int i=0; i<100000; i++) {
            Path partFile = new Path(modelPath + "/part-" + String.valueOf(100000 + i).substring(1, 6));
            if(fs.exists(partFile)) {
              files.add(partFile);
            } else {
              break;
            }
          }
        } else {
          files.add(dataPath);
        }

        LOG.info("loading model from " + path);
        for (Path file : files){
          SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);
          Text key = new Text();
          VectorWritable value = new VectorWritable();
          String strKey = null;
          Long actualKey = null;
          String firstSymbol = null;
          while (reader.next(key, value) != false) {
            strKey = key.toString();
            firstSymbol = strKey.substring(0, 1);
            try {
              actualKey = Long.valueOf(strKey.substring(1));
            } catch (Exception e) {
              actualKey = new Long(0);
            }

            if (firstSymbol.equals(OnlineCF.Settings.DFLT_MODEL_ITEM_DELIM)) {
              modelItemFactorizedValues.put(actualKey, new VectorWritable(value));
            } else if (firstSymbol.equals(OnlineCF.Settings.DFLT_MODEL_USER_DELIM)) {
              modelUserFactorizedValues.put(actualKey, new VectorWritable(value));
            } else if (firstSymbol.equals(OnlineCF.Settings.DFLT_MODEL_USER_FEATURES_DELIM)) {
              modelUserFeatures.put(actualKey, new VectorWritable(value));
            } else if (firstSymbol.equals(OnlineCF.Settings.DFLT_MODEL_ITEM_FEATURES_DELIM)) {
              modelItemFeatures.put(actualKey, new VectorWritable(value));
            } else if (firstSymbol.equals(OnlineCF.Settings.DFLT_MODEL_USER_MTX_FEATURES_DELIM)) {
              modelUserFeatureFactorizedValues = convertVectorWritable(value);
            } else if (firstSymbol.equals(OnlineCF.Settings.DFLT_MODEL_ITEM_MTX_FEATURES_DELIM)) {
              modelItemFeatureFactorizedValues = convertVectorWritable(value);
            } else {
              // unknown
              continue;
            }
          }
          reader.close();
        }
        LOG.info("loaded: " + modelUserFactorizedValues.size() + " users, "
                        + modelUserFeatures.size() + " user features, "
                        + modelItemFactorizedValues.size() + " items, "
                        + modelItemFeatures.size() + " item feature values");
      } catch (Exception e) {
        e.printStackTrace();
        this.isLazyLoadModel = false;
        this.modelPath = null;
        return false;
      }
    }
    return true;
  }

  private DoubleMatrix convertVectorWritable(VectorWritable value) {
    //format of array: matrix_rank, matrix_converted_to_vector
    DoubleVector vc = value.getVector();
    int matrix_rank = (int) vc.get(0);
    int matrix_size = vc.getLength()-1;
    LinkedList<DoubleVector> slices = new LinkedList<DoubleVector>();
    int offset = 1;
    while (offset < matrix_size) {
      slices.add(vc.slice(offset, matrix_rank));
      offset += matrix_rank;
    }
    DoubleMatrix res = new DenseDoubleMatrix((DoubleVector[])slices.toArray());
    return res;
  }

  @Override
  public double estimatePreference(long userId, long itemId) {
    if (isLazyLoadModel == false) {
      if (function == null) {
        Class<?> cls = conf.getClass(OnlineCF.Settings.CONF_ONLINE_UPDATE_FUNCTION, null);
        try {
          function = (OnlineUpdate.Function)(cls.newInstance());
        } catch (Exception e) {
          // set default function
        }
      }

      InputStructure e = new InputStructure();
      e.item = this.modelItemFactorizedValues.get(Long.valueOf(itemId));
      e.user = this.modelUserFactorizedValues.get(Long.valueOf(userId));
      e.itemFeatureFactorized = this.modelItemFeatureFactorizedValues;
      e.userFeatureFactorized = this.modelUserFeatureFactorizedValues;
      e.itemFeatures = this.modelItemFeatures.get(Long.valueOf(itemId));
      e.userFeatures = this.modelUserFeatures.get(Long.valueOf(userId));
      if (e.item == null || e.user == null) {
        return 0;
      }

      return function.predict(e);
    }
    return 0;
    
  }

  @Override
  public List<Preference<Long, Long>> getMostPreferredItems(long userId, int count) {
    Comparator<Preference<Long, Long>> scoreComparator = new Comparator<Preference<Long, Long>>() {
      
      @Override
      public int compare(Preference<Long, Long> arg0, Preference<Long, Long> arg1) {
        double difference = arg0.getValue().get() - arg1.getValue().get(); 
        return (int)(100000*difference);
      }
    };
    PriorityQueue<Preference<Long, Long>> queue = new PriorityQueue<Preference<Long, Long>>(count, scoreComparator);
    LinkedList<Preference<Long, Long>> results = new LinkedList<Preference<Long, Long>>();
    
    if (function == null) {
      Class<?> cls = conf.getClass(OnlineCF.Settings.CONF_ONLINE_UPDATE_FUNCTION, null);
      try {
        function = (OnlineUpdate.Function)(cls.newInstance());
      } catch (Exception e) {
        // set default function
      }
    }
    
    InputStructure e = new InputStructure();
    e.user = this.modelUserFactorizedValues.get(Long.valueOf(userId));
    e.userFeatureFactorized = this.modelUserFeatureFactorizedValues;
    e.userFeatures = this.modelUserFeatures.get(Long.valueOf(userId));
    e.itemFeatureFactorized = this.modelItemFeatureFactorizedValues;
    if (e.user == null) {
      return null;
    }
    
    double score = 0.0;
    for (Entry<Long, VectorWritable> item : modelItemFactorizedValues.entrySet()) {
      e.item = item.getValue();
      e.itemFeatures = this.modelItemFeatures.get(item.getKey());
      score = function.predict(e);
      queue.add(new Preference<Long, Long>(userId, item.getKey(), score));
    }
    results.addAll(queue);
    return results;
  }

  @Override
  public double calculateUserSimilarity(long user1, long user2) {
    VectorWritable usr1 = this.modelUserFactorizedValues.get(Long.valueOf(user1));
    VectorWritable usr2 = this.modelUserFactorizedValues.get(Long.valueOf(user2));
    if (usr1 == null || usr2 == null) {
      return Double.MAX_VALUE;
    }
    
    DoubleVector usr1Vector = usr1.getVector();
    DoubleVector usr2Vector = usr2.getVector();
    
    // Euclidean distance
    return Math.pow( usr1Vector
                    .subtract(usr2Vector)
                    .applyToElements(new SquareVectorFunction())
                    .sum() , 0.5);
  }

  @Override
  public List<Pair<Long, Double>> getMostSimilarUsers(long user, int count) {
    
    Comparator<Pair<Long, Double>> similarityComparator = new Comparator<Pair<Long, Double>>() {
      
      @Override
      public int compare(Pair<Long, Double> arg0, Pair<Long, Double> arg1) {
        double difference = arg0.getValue().doubleValue() - arg1.getValue().doubleValue(); 
        return (int)(100000*difference);
      }
    };
    PriorityQueue<Pair<Long, Double>> queue = new PriorityQueue<Pair<Long, Double>>(count, similarityComparator);
    LinkedList<Pair<Long, Double>> results = new LinkedList<Pair<Long, Double>>();
    for ( Long candidateUser : modelUserFactorizedValues.keySet() ) {
      double similarity = calculateUserSimilarity(user, candidateUser);
      Pair<Long, Double> targetUser = new Pair<Long, Double>(candidateUser, similarity);
      queue.add(targetUser);
    }
    results.addAll(queue);
    return results;
  }

  @Override
  public double calculateItemSimilarity(long item1, long item2) {
    VectorWritable itm1 = this.modelUserFactorizedValues.get(Long.valueOf(item1));
    VectorWritable itm2 = this.modelUserFactorizedValues.get(Long.valueOf(item2));
    if (itm1 == null || itm2 == null) {
      return Double.MAX_VALUE;
    }
    
    DoubleVector itm1Vector = itm1.getVector();
    DoubleVector itm2Vector = itm2.getVector();
    
    // Euclidean distance
    return Math.pow( itm1Vector
                      .subtract(itm2Vector)
                      .applyToElements(new SquareVectorFunction())
                      .sum() , 0.5);
  }

  @Override
  public List<Pair<Long, Double>> getMostSimilarItems(long item, int count) {
    
    Comparator<Pair<Long, Double>> similarityComparator = new Comparator<Pair<Long, Double>>() {
      
      @Override
      public int compare(Pair<Long, Double> arg0, Pair<Long, Double> arg1) {
        double difference = arg0.getValue().doubleValue() - arg1.getValue().doubleValue(); 
        return (int)(100000*difference);
      }
    };
    PriorityQueue<Pair<Long, Double>> queue = new PriorityQueue<Pair<Long, Double>>(count, similarityComparator);
    LinkedList<Pair<Long, Double>> results = new LinkedList<Pair<Long, Double>>();
    for ( Long candidateItem : modelItemFactorizedValues.keySet() ) {
      double similarity = calculateItemSimilarity(item, candidateItem);
      Pair<Long, Double> targetItem = new Pair<Long, Double>(candidateItem, similarity);
      queue.add(targetItem);
    }
    results.addAll(queue);
    return results;
  }

}
