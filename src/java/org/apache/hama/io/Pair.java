package org.apache.hama.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/** A Pair stands for (row, column) pair **/
public class Pair implements WritableComparable<Pair> {

  int row, col;
  
  public Pair() {}
  
  public Pair(int row_, int col_) {
    set(row_, col_);
  }
  
  public int getRow() { return row; }
  public int getColumn() { return col; }
  
  public void setRow(int row_) { row = row_; }
  public void setColumn(int col_) { col = col_; }
  public void set(int row_, int col_) {
    row = row_;
    col = col_;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    row = in.readInt();
    col = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(row);
    out.writeInt(col);
  }

  @Override
  public int compareTo(Pair p) {
    return row == p.row ? col - p.col : row - p.row;
  }

  @Override
  public boolean equals(Object obj) {
    Pair pair = (Pair)obj;
    return compareTo(pair) == 0;
  }

  @Override
  public int hashCode() {
    return row;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('(').append(row).append(',').append(col).append(')');
    return sb.toString();
  }

}
