package org.apache.hama;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hama.io.VectorWritable;
import org.apache.hama.util.Numeric;
import org.apache.log4j.Logger;

public class DenseVector extends VectorWritable implements Vector {
  static final Logger LOG = Logger.getLogger(Vector.class);

  public DenseVector() {
    this(null, new HbaseMapWritable<byte[], Cell>());
  }

  public DenseVector(final byte[] rowKey, final HbaseMapWritable<byte[], Cell> m) {
    this.row = row;
    this.cells = m;
  }

  public DenseVector(int row, RowResult rowResult) {
    this.cells = new HbaseMapWritable<byte[], Cell>();
    this.row = Numeric.intToBytes(row);
    for (Map.Entry<byte[], Cell> f : rowResult.entrySet()) {
      this.cells.put(f.getKey(), f.getValue());
    }
  }

  /**
   * Get the row for this Vector
   */
  public byte[] getRow() {
    return row;
  }

  public HbaseMapWritable<byte[], Cell> getCells() {
    return cells;
  }

  public void add(int index, double value) {
    // TODO Auto-generated method stub

  }

  public Vector add(double alpha, Vector v) {
    // TODO Auto-generated method stub
    return null;
  }

  public Vector add(Vector v2) {
    HbaseMapWritable<byte[], Cell> trunk = new HbaseMapWritable<byte[], Cell>();
    for (int i = 0; i < this.size(); i++) {
      double value = (this.get(i) + v2.get(i));
      Cell cValue = new Cell(String.valueOf(value), System.currentTimeMillis());
      trunk.put(Bytes.toBytes("column:" + i), cValue);
    }

    return new DenseVector(row, trunk);
  }

  public double dot(Vector v) {
    double cosine = 0.0;
    double q_i, d_i;
    for (int i = 0; i < Math.min(this.size(), v.size()); i++) {
      q_i = v.get(i);
      d_i = this.get(i);
      cosine += q_i * d_i;
    }
    return cosine / (this.getNorm2() * ((DenseVector) v).getNorm2());
  }

  public Vector scale(double alpha) {
    Set<byte[]> keySet = cells.keySet();
    Iterator<byte[]> it = keySet.iterator();

    while (it.hasNext()) {
      byte[] key = it.next();
      double oValue = Numeric.bytesToDouble(get(key).getValue());
      double nValue = oValue * alpha;
      Cell cValue = new Cell(String.valueOf(nValue), System.currentTimeMillis());
      cells.put(key, cValue);
    }

    return this;
  }

  public double get(int index) {
    return Numeric.bytesToDouble(cells.get(Numeric.getColumnIndex(index))
        .getValue());
  }

  public double norm(Norm type) {
    if (type == Norm.One)
      return getNorm1();
    else if (type == Norm.Two)
      return getNorm2();
    else if (type == Norm.TwoRobust)
      return getNorm2Robust();
    else
      return getNormInf();
  }

  public void set(int index, double value) {
    Cell cValue = new Cell(String.valueOf(value), System.currentTimeMillis());
    cells.put(Numeric.getColumnIndex(index), cValue);
  }

  public DenseVector set(Vector v) {
    return new DenseVector(((DenseVector) v).getRow(), ((DenseVector) v).getCells());
  }

  public double getNorm1() {
    double sum = 0.0;

    Set<byte[]> keySet = cells.keySet();
    Iterator<byte[]> it = keySet.iterator();

    while (it.hasNext()) {
      sum += Numeric.bytesToDouble(get(it.next()).getValue());
    }

    return sum;
  }

  public double getNorm2() {
    double square_sum = 0.0;

    Set<byte[]> keySet = cells.keySet();
    Iterator<byte[]> it = keySet.iterator();

    while (it.hasNext()) {
      double value = Numeric.bytesToDouble(get(it.next()).getValue());
      square_sum += value * value;
    }

    return Math.sqrt(square_sum);
  }

  public double getNorm2Robust() {
    // TODO Auto-generated method stub
    return 0;
  }

  public double getNormInf() {
    // TODO Auto-generated method stub
    return 0;
  }
}