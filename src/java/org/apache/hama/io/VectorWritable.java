package org.apache.hama.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.HbaseMapWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.Writable;
import org.apache.hama.AbstractBase;
import org.apache.hama.Vector;
import org.apache.log4j.Logger;

public class VectorWritable extends AbstractBase implements Writable,
    Map<byte[], Cell> {
  static final Logger LOG = Logger.getLogger(VectorWritable.class);
  public byte[] row;
  public HbaseMapWritable<byte[], Cell> cells;
  public int[] m_dims;
  public double[] m_vals;

  public void parse(Set<Entry<byte[], Cell>> entrySet) {
    this.cells = new HbaseMapWritable<byte[], Cell>();
    
    SortedMap<Integer, Double> m = new TreeMap<Integer, Double>();
    for (Map.Entry<byte[], Cell> f : entrySet) {
      m.put(getColumnIndex(f.getKey()), Double.parseDouble(Bytes.toString(f
          .getValue().getValue())));
      this.cells.put(f.getKey(), f.getValue());
    }

    this.m_dims = new int[m.keySet().size()];
    this.m_vals = new double[m.keySet().size()];

    int i = 0;
    for (Map.Entry<Integer, Double> f : m.entrySet()) {
      this.m_dims[i] = f.getKey();
      this.m_vals[i] = f.getValue();
      i++;
    }
  }

  public Cell put(@SuppressWarnings("unused")
  byte[] key, @SuppressWarnings("unused")
  Cell value) {
    throw new UnsupportedOperationException("VectorDatum is read-only!");
  }

  public Cell get(Object key) {
    return (Cell) this.cells.get(key);
  }

  public Cell remove(@SuppressWarnings("unused")
  Object key) {
    throw new UnsupportedOperationException("VectorDatum is read-only!");
  }

  public boolean containsKey(Object key) {
    return cells.containsKey(key);
  }

  public boolean containsValue(@SuppressWarnings("unused")
  Object value) {
    throw new UnsupportedOperationException("Don't support containsValue!");
  }

  public boolean isEmpty() {
    return cells.isEmpty();
  }

  public void clear() {
    throw new UnsupportedOperationException("VectorDatum is read-only!");
  }

  public Set<byte[]> keySet() {
    Set<byte[]> result = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    for (byte[] w : cells.keySet()) {
      result.add(w);
    }
    return result;
  }

  public Set<Map.Entry<byte[], Cell>> entrySet() {
    return Collections.unmodifiableSet(this.cells.entrySet());
  }

  public Collection<Cell> values() {
    ArrayList<Cell> result = new ArrayList<Cell>();
    for (Writable w : cells.values()) {
      result.add((Cell) w);
    }
    return result;
  }

  public void readFields(final DataInput in) throws IOException {
    this.row = Bytes.readByteArray(in);
    this.cells.readFields(in);
    parse(this.cells.entrySet());
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, this.row);
    this.cells.write(out);
  }

  public VectorWritable addition(byte[] bs, Vector v2) {
    // TODO Auto-generated method stub
    return null;
  }

  public void putAll(Map<? extends byte[], ? extends Cell> m) {
    // TODO Auto-generated method stub

  }

  /**
   * Get the Cell that corresponds to column
   */
  public Cell get(byte[] column) {
    return this.cells.get(column);
  }

  /**
   * Get the Cell that corresponds to column, using a String key
   */
  public Cell get(String key) {
    return get(Bytes.toBytes(key));
  }

  public int size() {
    // return this.cells.size();
    return m_dims.length;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("row=");
    sb.append(Bytes.toString(this.row));
    sb.append(", cells={");
    boolean moreThanOne = false;
    for (Map.Entry<byte[], Cell> e : this.cells.entrySet()) {
      if (moreThanOne) {
        sb.append(", ");
      } else {
        moreThanOne = true;
      }
      sb.append("(column=");
      sb.append(Bytes.toString(e.getKey()));
      sb.append(", timestamp=");
      sb.append(Long.toString(e.getValue().getTimestamp()));
      sb.append(", value=");
      byte[] v = e.getValue().getValue();
      if (Bytes.equals(e.getKey(), HConstants.COL_REGIONINFO)) {
        try {
          sb.append(Writables.getHRegionInfo(v).toString());
        } catch (IOException ioe) {
          sb.append(ioe.toString());
        }
      } else {
        sb.append(v);
      }
      sb.append(")");
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Row entry.
   */
  public class Entries implements Map.Entry<byte[], Cell> {
    private final byte[] column;
    private final Cell cell;

    Entries(byte[] row, Cell cell) {
      this.column = row;
      this.cell = cell;
    }

    public Cell setValue(@SuppressWarnings("unused")
    Cell c) {
      throw new UnsupportedOperationException("VectorDatum is read-only!");
    }

    public byte[] getKey() {
      return column;
    }

    public Cell getValue() {
      return cell;
    }
  }
}
