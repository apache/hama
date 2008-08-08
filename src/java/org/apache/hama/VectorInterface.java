package org.apache.hama;

/**
 * Basic vector interface.
 */
public interface VectorInterface {
  
  /**
   * @return size of the vector
   */
  public int size();

  /**
   * @param index
   * @return v(index)
   */
  public double get(int index); 
  
  /**
   * v(index) = value
   *  
   * @param index
   * @param value
   */
  public void set(int index, double value);
  
  /**
   * @param v
   * @return x = v 
   */
  public Vector set(Vector v);

  /**
   * v(index) += value
   *  
   * @param index
   * @param value
   */
  public void add(int index, double value);
  
  /**
   * @param alpha
   * @param v
   * @return  x = alpha*v + x
   */
  public boolean add(double alpha, Vector v);

  /**
   * @param v
   * @return x = v + x
   */
  public Vector add(Vector v);
  
  /**
   * @param v
   * @return x dot v
   */
  public double dot(Vector v);

  /**
   * Computes the given norm of the vector
   * 
   * @param type
   * @return norm of the vector
   */
  public double norm(Vector.Norm type);

  @Deprecated
  public double getValueAt(int index);
  
  @Deprecated
  public int getDimAt(int index);
  
  @Deprecated
  public double getL1Norm();
  
  @Deprecated
  public double getL2Norm();
}
