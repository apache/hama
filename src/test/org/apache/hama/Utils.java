package org.apache.hama;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Utils {
  private static HBaseConfiguration conf = new HBaseConfiguration();
  private static HBaseAdmin admin;

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage: org.apache.hama.Utils [list|delete-all]");
      System.out.println(" - list : listing the tables, related with Hama");
      System.out.println(" - delete-all : deleting the tables, related with Hama");
      System.exit(-1);
    }
    admin = new HBaseAdmin(conf);

    HTableDescriptor[] tables = admin.listTables();
    for (int i = 0; i < tables.length; i++) {
      if (isHamaTables(tables[i].getNameAsString())) {
        if (args[0].equals("list")) {
          System.out.println(tables[i].getNameAsString());
        } else if (args[0].equals("delete-all")) {
          deleteTable(tables[i]);
        }
      }
    }
  }

  private static void deleteTable(HTableDescriptor tableDescriptor)
      throws IOException {
    byte[] tableName = tableDescriptor.getName();
    while (admin.isTableEnabled(tableName)) {
      admin.disableTable(tableName);
    }

    admin.deleteTable(tableName);
  }

  private static boolean isHamaTables(String name) {
    if (name.equals(Constants.ADMINTABLE)
        || name.startsWith("DenseMatrix_rand")
        || name.startsWith("SparseMatrix_rand"))
      return true;

    return false;
  }
}
