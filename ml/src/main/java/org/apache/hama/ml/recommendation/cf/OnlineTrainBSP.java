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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;
import org.apache.hama.commons.io.MatrixWritable;
import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleMatrix;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.ml.recommendation.Preference;
import org.apache.hama.ml.recommendation.cf.function.OnlineUpdate;

public class OnlineTrainBSP extends
    BSP<Text, VectorWritable, Text, VectorWritable, MapWritable> {

  protected static Log LOG = LogFactory.getLog(OnlineTrainBSP.class);
  
  private String inputPreferenceDelim = null;
  private String inputUserDelim = null;
  private String inputItemDelim = null;

  private int ITERATION = 0;
  private int MATRIX_RANK = 0;
  private int SKIP_COUNT = 0;
  
  // randomly generated depending on matrix rank,
  // will be computed runtime and represents trained model
  // userId, factorized value
  private HashMap<String, VectorWritable> usersMatrix = new HashMap<String, VectorWritable>();
  // itemId, factorized value
  private HashMap<String, VectorWritable> itemsMatrix = new HashMap<String, VectorWritable>();
  // matrixRank, factorized value
  private DoubleMatrix userFeatureMatrix = null;
  private DoubleMatrix itemFeatureMatrix = null;

  // obtained from input data
  // will not change during execution
  private HashMap<String, VectorWritable> inpUsersFeatures = null;
  private HashMap<String, VectorWritable> inpItemsFeatures = null;
  
  private OnlineUpdate.Function function = null;
  
  // Input Preferences
  private ArrayList<Preference<String, String>> preferences = new ArrayList<Preference<String, String>>();
  private ArrayList<Integer> indexes = new ArrayList<Integer>();
  
  Random rnd = new Random();

  @Override
  public void setup(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer)
      throws IOException, SyncException, InterruptedException {

    Configuration conf = peer.getConfiguration();

    ITERATION = conf.getInt(OnlineCF.Settings.CONF_ITERATION_COUNT, OnlineCF.Settings.DFLT_ITERATION_COUNT);
    MATRIX_RANK = conf.getInt(OnlineCF.Settings.CONF_MATRIX_RANK, OnlineCF.Settings.DFLT_MATRIX_RANK);
    SKIP_COUNT = conf.getInt(OnlineCF.Settings.CONF_SKIP_COUNT, OnlineCF.Settings.DFLT_SKIP_COUNT);

    inputItemDelim = conf.get(OnlineCF.Settings.CONF_INPUT_ITEM_DELIM, OnlineCF.Settings.DFLT_ITEM_DELIM);
    inputUserDelim = conf.get(OnlineCF.Settings.CONF_INPUT_USER_DELIM, OnlineCF.Settings.DFLT_USER_DELIM);
    inputPreferenceDelim = conf.get(OnlineCF.Settings.CONF_INPUT_PREFERENCES_DELIM, OnlineCF.Settings.DFLT_PREFERENCE_DELIM);

    Class<?> cls = conf.getClass(OnlineCF.Settings.CONF_ONLINE_UPDATE_FUNCTION, null);
    try {
      function = (OnlineUpdate.Function)(cls.newInstance());
    } catch (Exception e) {
      // set default function
    }
  }

  @Override
  public void bsp(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer)
      throws IOException, SyncException, InterruptedException {
    LOG.info(peer.getPeerName() + ") collecting input data");
    // input partitioning begin,
    // because we used one file for all input data
    // and input data are different type
    HashSet<Text> requiredUserFeatures = null;
    HashSet<Text> requiredItemFeatures = null;
    collectInput(peer, requiredUserFeatures, requiredItemFeatures);
    // since we have used some delimiters for 
    // keys, HashPartitioner cannot partition
    // as we want, take user preferences and 
    // broadcast user features and item features
    askForFeatures(peer, requiredUserFeatures, requiredItemFeatures);
    peer.sync();

    requiredUserFeatures = null;
    requiredItemFeatures = null;
    sendRequiredFeatures(peer);
    peer.sync();

    collectFeatures(peer);
    LOG.info(peer.getPeerName() + ") collected: " + this.usersMatrix.size() + " users, "
                           + this.itemsMatrix.size() + " items, "
                           + this.preferences.size() + " preferences");
    // input partitioning end
    
    // calculation steps
    for (int i=0; i<ITERATION; i++) {
      computeValues();
      if ((i+1)%SKIP_COUNT == 0) {
        normalizeWithBroadcastingValues(peer);
      }
    }

    saveModel(peer);
  }

  private void normalizeWithBroadcastingValues(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer) 
          throws IOException, SyncException, InterruptedException {
    // normalize item factorized values
    // normalize user/item feature matrix
    peer.sync();
    normalizeItemFactorizedValues(peer);
    peer.sync();

    if (itemFeatureMatrix != null) {
      // item feature factorized values should be normalized
      normalizeMatrix(peer, itemFeatureMatrix, OnlineCF.Settings.MSG_ITEM_FEATURE_MATRIX, true);
      peer.sync();
    }

    if (userFeatureMatrix != null) {
      // user feature factorized values should be normalized
      normalizeMatrix(peer, userFeatureMatrix, OnlineCF.Settings.MSG_USER_FEATURE_MATRIX, true);
      peer.sync();
    }
  }

  private DoubleMatrix normalizeMatrix(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer,
      DoubleMatrix featureMatrix, IntWritable msgFeatureMatrix, boolean broadcast) 
          throws IOException, SyncException, InterruptedException {
    // send to master peer
    MapWritable msg = new MapWritable();
    MatrixWritable mtx = new MatrixWritable(featureMatrix);
    msg.put(msgFeatureMatrix, mtx);
    String master = peer.getPeerName(peer.getNumPeers()/2);
    peer.send(master, msg);
    peer.sync();

    // normalize
    DoubleMatrix res = null;
    if (peer.getPeerName().equals(master)) {
      res = new DenseDoubleMatrix(featureMatrix.getRowCount(),
                                  featureMatrix.getColumnCount(), 0);
      int incomingMsgCount = 0;
      while ( (msg = peer.getCurrentMessage()) != null) {
        MatrixWritable tmp = (MatrixWritable) msg.get(msgFeatureMatrix);
        res.add(tmp.getMatrix());
        incomingMsgCount++;
      }
      res.divide(incomingMsgCount);
    }

    if (broadcast) {
      if (peer.getPeerName().equals(master)) {
        // broadcast to all
        msg = new MapWritable();
        msg.put(msgFeatureMatrix, new MatrixWritable(res));
        // send to all
        for (String peerName : peer.getAllPeerNames()) {
          peer.send(peerName, msg);
        }
      }
      peer.sync();
      // receive normalized value from master
      msg = peer.getCurrentMessage();
      featureMatrix = ((MatrixWritable)msg.get(msgFeatureMatrix)).getMatrix();
    }
    return res;
  }

  private VectorWritable convertMatrixToVector(DoubleMatrix mat) {
    DoubleVector res = new DenseDoubleVector(mat.getRowCount()*mat.getColumnCount()+1);
    int idx = 0;
    res.set(idx, MATRIX_RANK);
    idx++;
    for (int i=0; i<mat.getRowCount(); i++) {
      for (int j=0; j<mat.getColumnCount(); j++) {
        res.set(idx, mat.get(i, j));
        idx++;
      }
    }
    return new VectorWritable(res);
  }

  private void normalizeItemFactorizedValues(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer) 
          throws IOException, SyncException, InterruptedException {
    // send item factorized matrices to selected peers
    sendItemFactorizedValues(peer);
    peer.sync();
    
    // receive item factorized matrices if this peer is selected and normalize them
    HashMap<Text, LinkedList<IntWritable>> senderList = new HashMap<Text, LinkedList<IntWritable>>();
    HashMap<Text, DoubleVector> normalizedValues = new HashMap<Text, DoubleVector>();
    getNormalizedItemFactorizedValues(peer, normalizedValues, senderList);
    
    // send back normalized values to senders
    sendTo(peer, senderList, normalizedValues);
    peer.sync();
    
    // receive already normalized and synced data
    receiveSyncedItemFactorizedValues(peer);
  }

  private void sendTo(BSPPeer<Text,VectorWritable,Text,VectorWritable,MapWritable> peer, 
      HashMap<Text, LinkedList<IntWritable>> senderList,
      HashMap<Text, DoubleVector> normalizedValues)
          throws IOException {

    for (Map.Entry<Text, DoubleVector> e : normalizedValues.entrySet()) {
      MapWritable msgTmp = new MapWritable();
      // send to interested peers
      msgTmp.put(OnlineCF.Settings.MSG_ITEM_MATRIX, e.getKey());
      msgTmp.put(OnlineCF.Settings.MSG_VALUE, new VectorWritable(e.getValue()));
      Iterator<IntWritable> iter = senderList.get(e.getKey()).iterator(); 
      while (iter.hasNext()) {
        peer.send(peer.getPeerName(iter.next().get()), msgTmp);
      }
    }
  }

  private void getNormalizedItemFactorizedValues(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer,
      HashMap<Text, DoubleVector> normalizedValues,
      HashMap<Text, LinkedList<IntWritable>> senderList) 
          throws IOException {

    HashMap<Text, Integer> normalizedValueCount = new HashMap<Text, Integer>();
    Text itemId = null;
    VectorWritable value = null;
    IntWritable senderId = null;
    MapWritable msg = new MapWritable();

    while ( (msg = peer.getCurrentMessage())!=null ) {
      itemId = (Text) msg.get(OnlineCF.Settings.MSG_ITEM_MATRIX);
      value = (VectorWritable) msg.get(OnlineCF.Settings.MSG_VALUE);
      senderId = (IntWritable) msg.get(OnlineCF.Settings.MSG_SENDER_ID);

      if (normalizedValues.containsKey(itemId) == false) {
        DenseDoubleVector tmp = new DenseDoubleVector(MATRIX_RANK, 0.0);
        normalizedValues.put(itemId, tmp);
        normalizedValueCount.put(itemId, 0);
        senderList.put(itemId, new LinkedList<IntWritable>());
      }

      normalizedValues.put(itemId, normalizedValues.get(itemId).add(value.getVector()));
      normalizedValueCount.put(itemId, normalizedValueCount.get(itemId)+1);
      senderList.get(itemId).add(senderId);
    }

    // normalize
    for (Map.Entry<Text, DoubleVector> e : normalizedValues.entrySet()) {
      double count = normalizedValueCount.get(e.getKey());
      e.setValue(e.getValue().multiply(1.0/count));
    }
  }

  private void receiveSyncedItemFactorizedValues(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer) 
          throws IOException {
    
    MapWritable msg = new MapWritable();
    Text itemId = null;
    // messages are arriving take them
    while ((msg = peer.getCurrentMessage()) != null) {
      itemId = (Text) msg.get(OnlineCF.Settings.MSG_ITEM_MATRIX);
      itemsMatrix.put(itemId.toString(), (VectorWritable)msg.get(OnlineCF.Settings.MSG_VALUE));
    }
  }

  private void sendItemFactorizedValues(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer) 
          throws IOException, SyncException, InterruptedException {
    int peerCount = peer.getNumPeers();
    // item factorized values should be normalized
    IntWritable peerId = new IntWritable(peer.getPeerIndex());

    for (Map.Entry<String, VectorWritable> item : itemsMatrix.entrySet()) {
      MapWritable msg = new MapWritable();
      msg.put(OnlineCF.Settings.MSG_ITEM_MATRIX, new Text(item.getKey()));
      msg.put(OnlineCF.Settings.MSG_VALUE, item.getValue());
      msg.put(OnlineCF.Settings.MSG_SENDER_ID, peerId);
      peer.send( peer.getPeerName(item.getKey().hashCode()%peerCount), msg);
    }
  }

  private void computeValues() {
    // shuffling indexes
    int idx = 0;
    int idxValue = 0;
    int tmp = 0;
    for (int i=indexes.size(); i>0; i--) {
      idx = Math.abs(rnd.nextInt())%i;
      idxValue = indexes.get(idx);
      tmp = indexes.get(i-1);
      indexes.set(i-1, idxValue);
      indexes.set(idx, tmp);
    }
    
    // compute values
    OnlineUpdate.InputStructure inp = new OnlineUpdate.InputStructure();
    OnlineUpdate.OutputStructure out = null;
    Preference<String, String> pref = null;
    for (Integer prefIdx : indexes) {
      pref = preferences.get(prefIdx);

      VectorWritable userFactorizedValues = usersMatrix.get(pref.getUserId());
      VectorWritable itemFactorizedValues = itemsMatrix.get(pref.getItemId());
      VectorWritable userFeatures = (inpUsersFeatures!=null)?inpUsersFeatures.get(pref.getUserId()):null;
      VectorWritable itemFeatures = (inpItemsFeatures!=null)?inpItemsFeatures.get(pref.getItemId()):null;

      inp.user = userFactorizedValues;
      inp.item = itemFactorizedValues;
      inp.expectedScore = pref.getValue();
      inp.userFeatures = userFeatures;
      inp.itemFeatures = itemFeatures;
      inp.userFeatureFactorized = userFeatureMatrix;
      inp.itemFeatureFactorized = itemFeatureMatrix;

      out = function.compute(inp);

      usersMatrix.put(pref.getUserId(), out.userFactorized);
      itemsMatrix.put(pref.getItemId(), out.itemFactorized);
      userFeatureMatrix = out.userFeatureFactorized;
      itemFeatureMatrix = out.itemFeatureFactorized;
    }
  }

  private void saveModel(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer) 
          throws IOException, SyncException, InterruptedException {

    // save user information
    LOG.info(peer.getPeerName() + ") saving " + usersMatrix.size() + " users");
    for (Map.Entry<String, VectorWritable> user : usersMatrix.entrySet()) {
      peer.write( new Text(OnlineCF.Settings.DFLT_MODEL_USER_DELIM + user.getKey()), user.getValue());
    }

    // broadcast item values, normalize and save
    sendItemFactorizedValues(peer);
    peer.sync();
    
    HashMap<Text, LinkedList<IntWritable>> senderList = new HashMap<Text, LinkedList<IntWritable>>();
    HashMap<Text, DoubleVector> normalizedValues = new HashMap<Text, DoubleVector>();
    getNormalizedItemFactorizedValues(peer, normalizedValues, senderList);

    saveItemFactorizedValues(peer, normalizedValues);

    // broadcast item and user feature matrix
    // normalize and save
    if (itemFeatureMatrix != null) {
      // save item features
      for (Map.Entry<String, VectorWritable> feature : inpItemsFeatures.entrySet()) {
        peer.write(new Text(OnlineCF.Settings.DFLT_MODEL_ITEM_FEATURES_DELIM+feature.getKey()), feature.getValue());
      }
      // item feature factorized values should be normalized
      DoubleMatrix res = normalizeMatrix(peer, itemFeatureMatrix, 
          OnlineCF.Settings.MSG_ITEM_FEATURE_MATRIX, false);
      
      if (res != null) {
        Text key = new Text(OnlineCF.Settings.DFLT_MODEL_ITEM_MTX_FEATURES_DELIM + 
            OnlineCF.Settings.MSG_ITEM_FEATURE_MATRIX.toString());
        peer.write(key, convertMatrixToVector(res));
      }
    }

    if (userFeatureMatrix != null) {
      // save user features
   // save item features
      for (Map.Entry<String, VectorWritable> feature : inpUsersFeatures.entrySet()) {
        peer.write(new Text(OnlineCF.Settings.DFLT_MODEL_USER_FEATURES_DELIM+feature.getKey()), feature.getValue());
      }
      // user feature factorized values should be normalized
      DoubleMatrix res = normalizeMatrix(peer, userFeatureMatrix, 
          OnlineCF.Settings.MSG_USER_FEATURE_MATRIX, false);
      
      if (res != null) {
        Text key = new Text(OnlineCF.Settings.DFLT_MODEL_USER_MTX_FEATURES_DELIM + 
            OnlineCF.Settings.MSG_USER_FEATURE_MATRIX.toString());
        peer.write(key, convertMatrixToVector(res));
      }
    }
  }

  private void saveItemFactorizedValues(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer,
      HashMap<Text, DoubleVector> normalizedValues) 
          throws IOException {
    LOG.info(peer.getPeerName() + ") saving " + normalizedValues.size() + " items");
    for (Map.Entry<Text, DoubleVector> item : normalizedValues.entrySet()) {
      peer.write( new Text(OnlineCF.Settings.DFLT_MODEL_ITEM_DELIM + item.getKey().toString()), 
          new VectorWritable(item.getValue()));
    }
  }

  private void sendRequiredFeatures(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer) 
      throws IOException, SyncException, InterruptedException {
    
    MapWritable msg = null;
    int senderId = 0;

    while ((msg = peer.getCurrentMessage()) != null) {
      senderId = ((IntWritable)msg.get(OnlineCF.Settings.MSG_SENDER_ID)).get();
      MapWritable resp = new MapWritable();
      if (msg.containsKey(OnlineCF.Settings.MSG_INP_ITEM_FEATURES)) {
        // send item feature
        String itemId = ((Text)msg.get(OnlineCF.Settings.MSG_INP_ITEM_FEATURES)).toString().substring(1);
        resp.put(OnlineCF.Settings.MSG_INP_ITEM_FEATURES, new Text(itemId));
        resp.put(OnlineCF.Settings.MSG_VALUE, inpItemsFeatures.get(itemId));
      } else if (msg.containsKey(OnlineCF.Settings.MSG_INP_USER_FEATURES)) {
        // send user feature
        String userId = ((Text)msg.get(OnlineCF.Settings.MSG_INP_USER_FEATURES)).toString().substring(1);
        resp.put(OnlineCF.Settings.MSG_INP_USER_FEATURES, new Text(userId));
        resp.put(OnlineCF.Settings.MSG_VALUE, inpUsersFeatures.get(userId));
      }
      peer.send(peer.getPeerName(senderId), resp);
    }
  }

  private void collectFeatures(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer) 
          throws IOException {
    // remove all features,
    // since we will get necessary features via messages
    inpItemsFeatures = new HashMap<String, VectorWritable>();
    inpUsersFeatures = new HashMap<String, VectorWritable>();

    MapWritable msg = null;
    int userFeatureSize = 0;
    int itemFeatureSize = 0;
    while ((msg = peer.getCurrentMessage()) != null) {
      if (msg.containsKey(OnlineCF.Settings.MSG_INP_ITEM_FEATURES)) {
        // send item feature
        String itemId = ((Text)msg.get(OnlineCF.Settings.MSG_INP_ITEM_FEATURES)).toString();
        inpItemsFeatures.put(itemId, (VectorWritable)msg.get(OnlineCF.Settings.MSG_VALUE));
        itemFeatureSize = ((VectorWritable)msg.get(OnlineCF.Settings.MSG_VALUE)).getVector().getLength();
      } else if (msg.containsKey(OnlineCF.Settings.MSG_INP_USER_FEATURES)) {
        // send user feature
        String userId = ((Text)msg.get(OnlineCF.Settings.MSG_INP_USER_FEATURES)).toString();
        inpUsersFeatures.put(userId, (VectorWritable)msg.get(OnlineCF.Settings.MSG_VALUE));
        userFeatureSize = ((VectorWritable)msg.get(OnlineCF.Settings.MSG_VALUE)).getVector().getLength();
      }
    }
    if (inpItemsFeatures.size() > 0) {
      itemFeatureMatrix = new DenseDoubleMatrix(MATRIX_RANK, itemFeatureSize, rnd);
    }
    if (inpUsersFeatures.size() > 0) {
      userFeatureMatrix = new DenseDoubleMatrix(MATRIX_RANK, userFeatureSize, rnd);
    }
  }

  private void askForFeatures(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer,
      HashSet<Text> requiredUserFeatures, 
      HashSet<Text> requiredItemFeatures) 
          throws IOException, SyncException, InterruptedException {
    int peerCount = peer.getNumPeers();
    int peerId = peer.getPeerIndex();
    
    if (requiredUserFeatures != null) {
      Iterator<Text> iter = requiredUserFeatures.iterator();
      Text key = null;
      while (iter.hasNext()) {
        MapWritable msg = new MapWritable();
        key = iter.next();
        msg.put(OnlineCF.Settings.MSG_INP_USER_FEATURES, key);
        msg.put(OnlineCF.Settings.MSG_SENDER_ID, new IntWritable(peerId));
        peer.send(peer.getPeerName(key.hashCode()%peerCount), msg);
      }
    }

    if (requiredItemFeatures != null) {
      Iterator<Text> iter = requiredItemFeatures.iterator();
      Text key = null;
      while (iter.hasNext()) {
        MapWritable msg = new MapWritable();
        key = iter.next();
        msg.put(OnlineCF.Settings.MSG_INP_ITEM_FEATURES, key);
        msg.put(OnlineCF.Settings.MSG_SENDER_ID, new IntWritable(peerId));
        peer.send(peer.getPeerName(key.hashCode()%peerCount), msg);
      }
    }
  }

  private void collectInput(
      BSPPeer<Text, VectorWritable, Text, VectorWritable, MapWritable> peer,
      HashSet<Text> requiredUserFeatures,
      HashSet<Text> requiredItemFeatures) 
          throws IOException {
    Text key = new Text();
    VectorWritable value = new VectorWritable();
    int counter = 0;

    requiredUserFeatures = new HashSet<Text>();
    requiredItemFeatures = new HashSet<Text>();

    while(peer.readNext(key, value)) {
      // key format: (0, 1..n)
      //  0 - delimiter, for type of key
      //  1..n - actaul key value
      String firstSymbol = key.toString().substring(0, 1);
      String actualId = key.toString().substring(1);

      if (firstSymbol.equals(inputPreferenceDelim)) {
        // parse as <k:userId, v:(itemId, score)>
        String itemId = Long.toString((long)value.getVector().get(0));
        String score = Double.toString(value.getVector().get(1));

        if (usersMatrix.containsKey(actualId) == false) {
          DenseDoubleVector vals = new DenseDoubleVector(MATRIX_RANK);
          for (int i=0; i<MATRIX_RANK; i++) {
            vals.set(i, rnd.nextDouble());
          }
          VectorWritable rndValues = new VectorWritable(vals);
          usersMatrix.put(actualId, rndValues);
        }

        if (itemsMatrix.containsKey(itemId) == false) {
          DenseDoubleVector vals = new DenseDoubleVector(MATRIX_RANK);
          for (int i=0; i<MATRIX_RANK; i++) {
            vals.set(i, rnd.nextDouble());
          }
          VectorWritable rndValues = new VectorWritable(vals);
          itemsMatrix.put(itemId, rndValues);
        }
        preferences.add(new Preference<String, String>(actualId, itemId, Double.parseDouble(score)));
        indexes.add(counter);
        
        // since we used HashPartitioner, 
        // in order to ask for input feature we need peer index
        // we can obtain peer index by using actual key
        requiredUserFeatures.add(new Text(inputUserDelim+actualId));
        requiredItemFeatures.add(new Text(inputItemDelim+itemId));
        counter++;
      } else if (firstSymbol.equals(inputUserDelim)) {
        // parse as <k:userId, v:(ArrayOfFeatureValues)>
        if (inpUsersFeatures == null) {
          inpUsersFeatures = new HashMap<String, VectorWritable>();          
        }
        inpUsersFeatures.put(actualId, value);
        
        
      } else if (firstSymbol.equals(inputItemDelim)) {
        // parse as <k:itemId, v:(ArrayOfFeatureValues)>
        if (inpItemsFeatures == null) {
          inpItemsFeatures = new HashMap<String, VectorWritable>();
        }
        inpItemsFeatures.put(actualId, value);
        
      } else {
        // just skip, maybe we should throw exception
        continue;
      }
    }
  }


}
