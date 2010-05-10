package org.apache.hama.examples;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hama.Constants;
import org.apache.hama.HamaCluster;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.examples.mapreduce.BlockMultMap;
import org.apache.hama.examples.mapreduce.BlockMultReduce;
import org.apache.hama.io.BlockID;
import org.apache.hama.matrix.DenseMatrix;
import org.apache.hama.matrix.DenseVector;
import org.apache.hama.matrix.Matrix;
import org.apache.hama.util.RandomVariable;

public class TestFileMatrixBlockMult extends HamaCluster {
  final static Log LOG = LogFactory.getLog(TestFileMatrixBlockMult.class
      .getName());
  private HamaConfiguration conf;
  private Path[] path = new Path[2];
  private String collectionTable;

  /**
   * @throws UnsupportedEncodingException
   */
  public TestFileMatrixBlockMult() throws UnsupportedEncodingException {
    super();
  }

  public void setUp() throws Exception {
    super.setUp();

    conf = getConf();
    HBaseAdmin admin = new HBaseAdmin(conf);
    collectionTable = "collect_" + RandomVariable.randMatrixPath();
    HTableDescriptor desc = new HTableDescriptor(collectionTable);
    desc.addFamily(new HColumnDescriptor(Bytes.toBytes(Constants.BLOCK)));
    admin.createTable(desc);
  }

  public void createFiles() throws IOException {
    Configuration conf = new Configuration();
    LocalFileSystem fs = new LocalFileSystem();
    fs.setConf(conf);
    fs.getRawFileSystem().setConf(conf);

    for (int i = 0; i < 2; i++) {
      path[i] = new Path(System.getProperty("test.build.data", ".") + "/tmp"
          + i + ".seq");
      SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path[i],
          IntWritable.class, MapWritable.class, CompressionType.BLOCK);

      MapWritable value = new MapWritable();
      value.put(new IntWritable(0), new DoubleWritable(0.5));
      value.put(new IntWritable(1), new DoubleWritable(0.1));
      value.put(new IntWritable(2), new DoubleWritable(0.5));
      value.put(new IntWritable(3), new DoubleWritable(0.1));

      writer.append(new IntWritable(0), value);
      writer.append(new IntWritable(1), value);
      writer.append(new IntWritable(2), value);
      writer.append(new IntWritable(3), value);

      writer.close();
    }

    FileSystem xfs = path[0].getFileSystem(conf);
    SequenceFile.Reader reader1 = new SequenceFile.Reader(xfs, path[0], conf);
    // read first value from reader1
    IntWritable key = new IntWritable();
    MapWritable val = new MapWritable();
    reader1.next(key, val);

    assertEquals(key.get(), 0);
  }

  public void testFileMatrixMult() throws IOException {
    createFiles();
    collectBlocksFromFile(path[0], true, collectionTable, conf);
    collectBlocksFromFile(path[1], false, collectionTable, conf);

    Matrix result = new DenseMatrix(conf, 4, 4);
    Job job = new Job(conf, "multiplication MR job : " + result.getPath());

    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(Constants.BLOCK));

    TableMapReduceUtil.initTableMapperJob(collectionTable, scan,
        BlockMultMap.class, BlockID.class, BytesWritable.class, job);
    TableMapReduceUtil.initTableReducerJob(result.getPath(),
        BlockMultReduce.class, job);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    verifyMultResult(result);
  }

  private void verifyMultResult(Matrix result) throws IOException {
    double[][] a = new double[][] { { 0.5, 0.1, 0.5, 0.1 },
        { 0.5, 0.1, 0.5, 0.1 }, { 0.5, 0.1, 0.5, 0.1 }, { 0.5, 0.1, 0.5, 0.1 } };
    double[][] b = new double[][] { { 0.5, 0.1, 0.5, 0.1 },
        { 0.5, 0.1, 0.5, 0.1 }, { 0.5, 0.1, 0.5, 0.1 }, { 0.5, 0.1, 0.5, 0.1 } };
    double[][] c = new double[4][4];

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; j++) {
        for (int k = 0; k < 4; k++) {
          c[i][k] += a[i][j] * b[j][k];
        }
      }
    }

    for (int i = 0; i < result.getRows(); i++) {
      for (int j = 0; j < result.getColumns(); j++) {
        double gap = (c[i][j] - result.get(i, j));
        assertTrue(gap < 0.000001 || gap < -0.000001);
      }
    }
  }

  private static void collectBlocksFromFile(Path path, boolean b,
      String collectionTable, HamaConfiguration conf) throws IOException {
    Job job = new Job(conf, "Blocking MR job" + path);
    job.setMapperClass(MyMapper.class);
    job.setMapOutputKeyClass(BlockID.class);
    job.setMapOutputValueClass(MapWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, path);

    job.getConfiguration().set(MyMapper.BLOCK_SIZE, String.valueOf(2));
    job.getConfiguration().set(MyMapper.ROWS, String.valueOf(4));
    job.getConfiguration().set(MyMapper.COLUMNS, String.valueOf(4));
    job.getConfiguration().setBoolean(MyMapper.MATRIX_POS, b);
    
    TableMapReduceUtil.initTableReducerJob(collectionTable,
        org.apache.hama.examples.mapreduce.CollectBlocksReducer.class, job);

    try {
      job.waitForCompletion(true);
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  public static class MyMapper extends
      Mapper<IntWritable, MapWritable, BlockID, MapWritable> implements
      Configurable {
    private Configuration conf = null;
    /** Parameter of the path of the matrix to be blocked * */
    public static final String BLOCK_SIZE = "hama.blocking.size";
    public static final String ROWS = "hama.blocking.rows";
    public static final String COLUMNS = "hama.blocking.columns";
    public static final String MATRIX_POS = "a.or.b";

    private int mBlockNum;
    private int mBlockRowSize;
    private int mBlockColSize;
    private int mRows;
    private int mColumns;

    public void map(IntWritable key, MapWritable value, Context context)
        throws IOException, InterruptedException {
      int startColumn, endColumn, blkRow = key.get() / mBlockRowSize, i = 0;
      DenseVector dv = new DenseVector(key.get(), value);

      do {
        startColumn = i * mBlockColSize;
        endColumn = startColumn + mBlockColSize - 1;
        if (endColumn >= mColumns) // the last sub vector
          endColumn = mColumns - 1;
        context.write(new BlockID(blkRow, i), dv.subVector(startColumn,
            endColumn).getEntries());

        i++;
      } while (endColumn < (mColumns - 1));
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;

      mBlockNum = conf.getInt(BLOCK_SIZE, 2);
      mRows = conf.getInt(ROWS, 4);
      mColumns = conf.getInt(COLUMNS, 4);

      mBlockRowSize = mRows / mBlockNum;
      mBlockColSize = mColumns / mBlockNum;
    }
  }
}
