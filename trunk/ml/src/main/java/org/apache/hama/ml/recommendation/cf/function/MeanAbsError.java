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
package org.apache.hama.ml.recommendation.cf.function;

import org.apache.hama.commons.io.VectorWritable;
import org.apache.hama.commons.math.DenseDoubleMatrix;
import org.apache.hama.commons.math.DenseDoubleVector;
import org.apache.hama.commons.math.DoubleMatrix;
import org.apache.hama.commons.math.DoubleVector;
import org.apache.hama.ml.recommendation.cf.function.OnlineUpdate.InputStructure;
import org.apache.hama.ml.recommendation.cf.function.OnlineUpdate.OutputStructure;

/**
 * <h3>Mean absolute error function for Online CF
 * as described in paper see #HAMA-612</h3>
 * Input:
 * <lu> 
 *   <li>User features matrix       - x (n x C)</li>
 *   <li>Product features vector    - y (n x D)</li>
 * </lu>
 *
 * Initialize randomly:
 * <lu>
 *   <li>User factorized values α (n x k);</li>
 *   <li>Item factorized values β (n x k);</li>
 *   <li>User feature factorized value μ (k x C);</li> 
 *   <li>Item feature factorized value ν ∈ (k x D);</li>
 * </lu>
 * 
 * Algorithm:
 * <pre>for each (a, b, r) do:
 *   Compute R ← (α_al + μ_l: * x_a:)(β_bl + ν_l: * y_b:)
 *   for l=1 to MATRIX_RANK do:
 *     α_al ← α_al + 2τ * (β_bl + ν_l: * y_b:)(r − R)
 *     β_bl ← β_bl + 2τ * (α_al + μ_l: * x_a:)(r − R)
 *     for c = 1 to C do:
 *       μ_lc ← μ_lc + 2τ * x_ac (β_bl + ν_l: * y_b:)(r − R)
 *     for d = 1 to D do:
 *       ν_ld ← ν_ld + 2τ * y_bd (α_al + μ_l: * x_a:)(r − R)
 * 
 * Output: α, β, μ, ν
 * </pre>
 * 
 * Below class implements computation phase under <b>for each (a,b,r) do</b> block 
 */
public class MeanAbsError implements OnlineUpdate.Function{
  private static final double TETTA = 0.01; 
  private DoubleVector zeroVector = null;
  @Override
  public OutputStructure compute(InputStructure e) {
    OutputStructure res = new OutputStructure();

    int rank = e.user.getVector().getLength();
    if (zeroVector == null) {
      zeroVector = new DenseDoubleVector(rank, 0);
    }
    // below vectors are all size of MATRIX_RANK
    DoubleVector vl_yb_item = zeroVector;
    DoubleVector ml_xa_user = zeroVector;
    DoubleVector bbl_vl_yb = null;
    DoubleVector aal_ml_xa = null;

    boolean isAvailableUserFeature = (e.userFeatures!=null);
    boolean isAvailableItemFeature = (e.itemFeatures!=null);

    if (isAvailableItemFeature) {
      DoubleVector yb = e.itemFeatures.getVector();
      vl_yb_item = e.itemFeatureFactorized.multiplyVector(yb);
    }
    
    if (isAvailableUserFeature) {
      DoubleVector xa = e.userFeatures.getVector();
      ml_xa_user = e.userFeatureFactorized.multiplyVector(xa);
    }
    
    bbl_vl_yb = e.item.getVector().add(vl_yb_item);
    aal_ml_xa = e.user.getVector().add(ml_xa_user);
    
    //calculated score
    double calculatedScore = aal_ml_xa.multiply(bbl_vl_yb).sum();
    double expectedScore = e.expectedScore.get();
    double scoreDifference = 0.0;
    scoreDifference = expectedScore - calculatedScore;
    
    // β_bl ← β_bl + 2τ * (α_al + μ_l: * x_a:)(r − R)
    // items ← item + itemFactorization (will be used later)
    DoubleVector itemFactorization = aal_ml_xa.multiply(2*TETTA*scoreDifference);
    DoubleVector items = e.item.getVector().add( itemFactorization );
    res.itemFactorized = new VectorWritable(items);
    
    // α_al ← α_al + 2τ * (β_bl + ν_l: * y_b:)(r − R)
    // users ← user + userFactorization (will be used later)
    DoubleVector userFactorization = bbl_vl_yb.multiply(2*TETTA*scoreDifference);
    DoubleVector users = e.user.getVector().add( userFactorization );
    res.userFactorized = new VectorWritable(users);

    // for d = 1 to D do:
    //   ν_ld ← ν_ld + 2τ * y_bd (α_al + μ_l: * x_a:)(r − R)
    // for c = 1 to C do:
    //   μ_lc ← μ_lc + 2τ * x_ac (β_bl + ν_l: * y_b:)(r − R)
    // 
    // ν_ld, μ_lc (V) is type of matrix, 
    // but 2τ * y_bd (α_al + μ_l: * x_a:)(r − R) (M later) will be type of vector
    // in order to add vector values to matrix
    // we create matrix with MMMMM and then transpose it.
    DoubleMatrix tmp = null;
    if (isAvailableItemFeature) {
      DoubleVector[] Mtransposed = new DenseDoubleVector[rank];
      for (int i = 0; i<rank; i++) {
        Mtransposed[i] = e.itemFeatureFactorized.getRowVector(i).multiply(aal_ml_xa.get(i));
      }
      tmp = new DenseDoubleMatrix(Mtransposed);
      tmp = tmp.multiply(2*TETTA*scoreDifference);
      res.itemFeatureFactorized = e.itemFeatureFactorized.add(tmp);
    }

    if (isAvailableUserFeature) {
      DoubleVector[] Mtransposed = new DenseDoubleVector[rank];
      for (int i = 0; i<rank; i++) {
        Mtransposed[i] = e.userFeatureFactorized.getRowVector(i).multiply(bbl_vl_yb.get(i));
      }
      tmp = new DenseDoubleMatrix(Mtransposed);
      tmp = tmp.multiply(2*TETTA*scoreDifference);
      res.userFeatureFactorized = e.userFeatureFactorized.add(tmp);
    }
    return res;
  }
  @Override
  public double predict(InputStructure e) {
    int rank = e.user.getVector().getLength();
    if (zeroVector == null) {
      zeroVector = new DenseDoubleVector(rank, 0);
    }
    // below vectors are all size of MATRIX_RANK
    DoubleVector vl_yb_item = zeroVector;
    DoubleVector ml_xa_user = zeroVector;
    DoubleVector bbl_vl_yb = null;
    DoubleVector aal_ml_xa = null;

    boolean isAvailableUserFeature = (e.userFeatures!=null);
    boolean isAvailableItemFeature = (e.itemFeatures!=null);

    if (isAvailableItemFeature) {
      DoubleVector yb = e.itemFeatures.getVector();
      vl_yb_item = e.itemFeatureFactorized.multiplyVector(yb);
    }

    if (isAvailableUserFeature) {
      DoubleVector xa = e.userFeatures.getVector();
      ml_xa_user = e.userFeatureFactorized.multiplyVector(xa);
    }
    
    bbl_vl_yb = e.item.getVector().add(vl_yb_item);
    aal_ml_xa = e.user.getVector().add(ml_xa_user);
    
    //calculated score
    double calculatedScore = aal_ml_xa.multiply(bbl_vl_yb).sum();
    return calculatedScore;
  }
}
