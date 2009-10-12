package org.apache.hama;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.HBaseClusterTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHbaseClient extends HBaseClusterTestCase {
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] VALUE = Bytes.toBytes("value");
  private static final byte[] MISSING_ROW = Bytes.toBytes("missingrow");

  private HTableDescriptor desc = null;
  private HTable table = null;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestHbaseClient() throws UnsupportedEncodingException {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.desc = new HTableDescriptor("testGet");
    desc.addFamily(new HColumnDescriptor(FAMILY));
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(desc);
    table = new HTable(conf, desc.getName());
  }

  public void testGet_EmptyTable() throws IOException {
    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertTrue(r.isEmpty());
  }

  public void testGet_NonExistentRow() throws IOException {
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    table.put(put);
    System.out.println("Row put");

    Get get = new Get(ROW);
    get.addFamily(FAMILY);
    Result r = table.get(get);
    assertFalse(r.isEmpty());
    assertEquals(Bytes.toString(VALUE), Bytes.toString(r.getValue(FAMILY, QUALIFIER)));
    System.out.println("Row retrieved successfully");

    get = new Get(MISSING_ROW);
    get.addFamily(FAMILY);
    r = table.get(get);
    assertTrue(r.isEmpty());
    System.out.println("Row missing as it should be");
  }
}
