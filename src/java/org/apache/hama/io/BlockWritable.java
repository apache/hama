package org.apache.hama.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hama.SubMatrix;
import org.apache.hama.util.BytesUtil;

public class BlockWritable implements Writable, Map<Integer, BlockEntry> {

  public Integer row;
  public BlockMapWritable<Integer, BlockEntry> entries;

  public BlockWritable() {
    this(new BlockMapWritable<Integer, BlockEntry>());
  }

  public BlockWritable(BlockMapWritable<Integer, BlockEntry> entries) {
    this.entries = entries;
  }

  public BlockWritable(int i, int j, SubMatrix mult) throws IOException {
    this.row = i;
    BlockMapWritable<Integer, BlockEntry> tr = new BlockMapWritable<Integer, BlockEntry>();
    tr.put(j, new BlockEntry(mult));
    this.entries = tr;
  }

  public int size() {
    return this.entries.size();
  }
  
  public SubMatrix get(int key) throws IOException {
    return this.entries.get(key).getValue();
  }
  
  public BlockEntry put(Integer key, BlockEntry value) {
    throw new UnsupportedOperationException("VectorWritable is read-only!");
  }

  public BlockEntry get(Object key) {
    return this.entries.get(key);
  }

  public BlockEntry remove(Object key) {
    throw new UnsupportedOperationException("VectorWritable is read-only!");
  }

  public boolean containsKey(Object key) {
    return entries.containsKey(key);
  }

  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("Don't support containsValue!");
  }

  public boolean isEmpty() {
    return entries.isEmpty();
  }

  public void clear() {
    throw new UnsupportedOperationException("VectorDatum is read-only!");
  }

  public Set<Integer> keySet() {
    Set<Integer> result = new TreeSet<Integer>();
    for (Integer w : entries.keySet()) {
      result.add(w);
    }
    return result;
  }

  public Set<Map.Entry<Integer, BlockEntry>> entrySet() {
    return Collections.unmodifiableSet(this.entries.entrySet());
  }

  public Collection<BlockEntry> values() {
    ArrayList<BlockEntry> result = new ArrayList<BlockEntry>();
    for (Writable w : entries.values()) {
      result.add((BlockEntry) w);
    }
    return result;
  }

  public void readFields(final DataInput in) throws IOException {
    this.row = BytesUtil.bytesToInt(Bytes.readByteArray(in));
    this.entries.readFields(in);
  }

  public void write(final DataOutput out) throws IOException {
    Bytes.writeByteArray(out, BytesUtil.intToBytes(this.row));
    this.entries.write(out);
  }

  public void putAll(Map<? extends Integer, ? extends BlockEntry> m) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  /**
   * 
   * The inner class for an entry of row.
   * 
   */
  public static class Entries implements Map.Entry<byte[], BlockEntry> {

    private final byte[] column;
    private final BlockEntry entry;

    Entries(byte[] column, BlockEntry entry) {
      this.column = column;
      this.entry = entry;
    }

    public BlockEntry setValue(BlockEntry c) {
      throw new UnsupportedOperationException("VectorWritable is read-only!");
    }

    public byte[] getKey() {
      byte[] key = column;
      return key;
    }

    public BlockEntry getValue() {
      return entry;
    }
  }
}
