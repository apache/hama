####################################################################
# Hama Pipes Examples README                                       #
####################################################################
# Hama Pipes includes the following three examples:                #
# - 1) Summation                                                   #
# - 2) PiEstimator                                                 #
# - 3) MatrixMultiplication                                        #
####################################################################

To run the examples, first compile them:

% mvn install

####################################################################
# 1) Summation Example                                             #
####################################################################

First copy summation binary to dfs

% hadoop fs -put c++/target/native/examples/summation \
 /examples/bin/summation

Generate input data and copy to dfs

% echo -e "key1\t1.0\nkey2\t2.0\nkey3\t3.0\nkey4\t4.0\n\
key5\t5.0\nkey6\t6.0\nkey7\t7.0\nkey8\t8.0\nkey9\t9.0\n\
key10\t10.0" > summation.txt && hadoop fs -put summation.txt \
 /examples/input/summation/input.txt && rm summation.txt

Run summation example

% hama pipes \
 -conf c++/src/main/native/examples/conf/summation.xml \
 -input /examples/input/summation \
 -output /examples/output/summation

View input and output data

% hadoop fs -cat /examples/input/summation/input.txt
% hama seqdumper -file /examples/output/summation/part-00000

You should see
# Input Path: /examples/output/summation/part-00000
# Key class: class org.apache.hadoop.io.Text 
# Value Class: class org.apache.hadoop.io.DoubleWritable
# Key: Sum: Value: 55.0
# Count: 1

Delete input and output folder

% hadoop fs -rmr /examples/input/summation
% hadoop fs -rmr /examples/output/summation

####################################################################
# 2) PiEstimator Example                                           #
####################################################################

First copy piestimator binary to dfs

% hadoop fs -put c++/target/native/examples/piestimator \
 /examples/bin/piestimator

Run piestimator example because no input data is needed

% hama pipes \
 -conf c++/src/main/native/examples/conf/piestimator.xml \
 -output /examples/output/piestimator

View output data

% hama seqdumper -file /examples/output/piestimator/part-00001

You should see
# Input Path: /examples/output/piestimator/part-00001
# Key class: class org.apache.hadoop.io.Text 
# Value Class: class org.apache.hadoop.io.DoubleWritable
# Key: Estimated value of PI: Value: 3.139116
# Count: 1

Delete output folder

% hadoop fs -rmr /examples/output/piestimator

####################################################################
# 3) MatrixMultiplication Example                                  #
####################################################################

First copy matrixmultiplication binary to dfs

% hadoop fs -put c++/target/native/examples/matrixmultiplication \
 /examples/bin/matrixmultiplication

Generate input data

% hama jar dist/target/hama-*/hama-*/hama-examples-*.jar \
  gen vectorwritablematrix 4 4 \
  /examples/input/matrixmultiplication/MatrixA.seq \
  false true 0 10 0

% hama jar dist/target/hama-*/hama-*/hama-examples-*.jar \
  gen vectorwritablematrix 4 4 \
  /examples/input/matrixmultiplication/MatrixB_transposed.seq \
  false true 0 10 0

Run matrixmultiplication example

% hama pipes \
 -conf c++/src/main/native/examples/conf/matrixmultiplication.xml \
 -output /examples/output/matrixmultiplication

View input and output data

% hama seqdumper \
 -file /examples/input/matrixmultiplication/MatrixA.seq

% hama seqdumper \
 -file  \
 /examples/input/matrixmultiplication/MatrixB_transposed.seq

% hama seqdumper \
 -file /examples/output/matrixmultiplication/part-00001

Delete output folder

% hadoop fs -rmr /examples/output/matrixmultiplication

####################################################################
