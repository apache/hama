package org.apache.hama;

import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.examples.*;

public class Benchmarks {

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: <size> <max-iterations>");
      System.exit(-1);
    }

    HamaConfiguration conf = new HamaConfiguration();
    System.out.println("Creating random matrix");
    DenseMatrix rand = RandomMatrix.random_mapred(conf, Integer
        .parseInt(args[0]), Integer.parseInt(args[0]));

    double start = System.currentTimeMillis();
    rand.jacobiEigenValue(Integer.parseInt(args[1]));
    double end = System.currentTimeMillis();
    System.out.println("Runtime: " + ((end - start)) * 1000 + " sec");
  }

}
