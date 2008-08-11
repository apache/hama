package org.apache.hama;

/**
 * Basic vector interface.
 */
public interface VectorInterface {

  /**
   * Size of the vector
   * 
   * @return size of the vector
   */
  public int size();

  /**
   * Gets the value of index
   * 
   * @param index
   * @return v(index)
   */
  public double get(int index);

  /**
   * Sets the value of index
   * 
   * @param index
   * @param value
   */
  public void set(int index, double value);

  /**
   * Sets the vector
   * 
   * @param v
   * @return x = v
   */
  public Vector set(Vector v);

  /**
   * Adds the value to v(index)
   * 
   * @param index
   * @param value
   */
  public void add(int index, double value);

  /**
   * x = alpha*v + x
   * 
   * @param alpha
   * @param v
   * @return x = alpha*v + x
   */
  public boolean add(double alpha, Vector v);

  /**
   * x = v + x
   * 
   * @param v
   * @return x = v + x
   */
  public Vector add(Vector v);

  /**
   * x dot v
   * 
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
  public double norm(Norm type);

  /**
   * Supported vector-norms.
   */
  enum Norm {

    /** Sum of the absolute values of the entries */
    One,

    /** The root of sum of squares */
    Two,

    /**
     * As the 2 norm may overflow, an overflow resistant version is also
     * available. Note that it may be slower.
     */
    TwoRobust,

    /** Largest entry in absolute value */
    Infinity
  }
}
