package org.apache.hama;

import org.apache.hama.io.VectorDatum;

/**
 * A vector. Features are dimension-value pairs. This class implements a
 * simple dictionary data structure to map dimensions onto their values. Note
 * that for convenience, features do not have be sorted according to their
 * dimensions at this point. The SVMLightTrainer class has an option for sorting
 * input vectors prior to training.
 */
public interface VectorInterface {
  
  public double getValueAt(int index);
  
  public int getDimAt(int index);
  
  public int size();
  
  public double getL1Norm();
  
  public double getL2Norm();
  
  public double getCosine(Vector v);
  
  public VectorDatum addition(byte[] bs, Vector v2);
  
  // TODO: save, copy,...,etc
  
}
