package org.apache.hama.matrix.algebra;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hama.Constants;
import org.apache.hama.io.VectorUpdate;
import org.apache.hama.util.BytesUtil;

/**
 * The matrix will be modified during computing eigen value. So a new matrix
 * will be created to prevent the original matrix being modified. To reduce the
 * network transfer, we copy the "column" family in the original matrix to a
 * "eicol" family. All the following modification will be done over "eicol"
 * family.
 * 
 * And the output Eigen Vector Arrays "eivec", and the output eigen value array
 * "eival:value", and the temp status array "eival:changed", "eival:ind" will be
 * created.
 * 
 * Also "eival:state" will record the state of the rotation state of a matrix
 */
public class JacobiInitMap extends TableMapper<ImmutableBytesWritable, Put> {

  public void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    int row, col;
    row = BytesUtil.getRowIndex(key.get());
    VectorUpdate vu = new VectorUpdate(row);

    double val;
    double maxVal = Double.MIN_VALUE;
    int maxInd = row + 1;

    boolean init = true;

    NavigableMap<byte[], byte[]> map = value
        .getFamilyMap(Constants.COLUMNFAMILY);
    for (Map.Entry<byte[], byte[]> e : map.entrySet()) {
      val = BytesUtil.bytesToDouble(e.getValue());
      col = BytesUtil.bytesToInt(e.getKey());
      // copy the original matrix to "EICOL" family
      vu.put(Constants.EICOL, col, val);
      // make the "EIVEC" a dialog matrix
      vu.put(Constants.EIVEC, col, col == row ? 1 : 0);
      if (col == row) {
        vu.put(Constants.EI, Constants.EIVAL, val);
      }
      // find the max index
      if (col > row) {
        if (init) {
          maxInd = col;
          maxVal = val;
          init = false;
        } else {
          if (Math.abs(val) > Math.abs(maxVal)) {
            maxVal = val;
            maxInd = col;
          }
        }
      }
    }

    // index array
    vu.put(Constants.EI, Constants.EIIND, String.valueOf(maxInd));
    // Changed Array set to be true during initialization
    vu.put(Constants.EI, Constants.EICHANGED, String.valueOf(1));
    context.write(key, vu.getPut());
  }
}
