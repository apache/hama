####################################################################
# Hama Pipes Examples                                              #
####################################################################
# - Summation                                                      #
# - PiEstimator                                                    #
# - MatrixMultiplication                                           #
####################################################################

To run the examples, first compile them:

% mvn install 

and then copy the binaries to dfs:

% hadoop fs -put c++/target/native/examples/summation \
 /examples/bin/summation

create an input directory with text files:

% hadoop fs -put my-data in-dir

and run the word count example:

% hama pipes \
 -conf c++/src/main/native/examples/conf/summation.xml \
 -input in-dir -output out-dir

####################################################################

# Summation Example

% hadoop fs -put c++/target/native/examples/summation \
 /examples/bin/summation

% echo -e "key1\t1.0\nkey2\t2.0\nkey3\t3.0\nkey4\t4.0\n\
key5\t5.0\nkey6\t6.0\nkey7\t7.0\nkey8\t8.0\nkey9\t9.0\n\
key10\t10.0" > summation.txt && hadoop fs -put summation.txt \
 /examples/input/summation/input.txt && rm summation.txt

% hama pipes \
 -conf c++/src/main/native/examples/conf/summation.xml \
 -input /examples/input/summation \
 -output /examples/output/summation

% hadoop fs -cat /examples/input/summation/input.txt
% hadoop fs -cat /examples/output/summation/part-00000

% hadoop fs -rmr /examples/input/summation
% hadoop fs -rmr /examples/output/summation

####################################################################

# PiEstimator Example

% hadoop fs -put c++/target/native/examples/piestimator \
 /examples/bin/piestimator

% hama pipes \
 -conf c++/src/main/native/examples/conf/piestimator.xml \
 -output /examples/output/piestimator

% hadoop fs -cat /examples/output/piestimator/part-00001

% hadoop fs -rmr /examples/output/piestimator

####################################################################

# MatrixMultiplication Example

% hadoop fs -put c++/target/native/examples/matrixmultiplication \
 /examples/bin/matrixmultiplication

% hadoop fs -put c++/src/main/native/examples/input/MatrixA.seq \
 /examples/input/matrixmultiplication/MatrixA.seq

% hadoop fs -put \
 c++/src/main/native/examples/input/MatrixB_transposed.seq \
 /examples/input/matrixmultiplication/MatrixB_transposed.seq

% hama pipes \
 -conf c++/src/main/native/examples/conf/matrixmultiplication.xml \
 -output /examples/output/matrixmultiplication

% hama seqdumper \
 -seqFile /examples/input/matrixmultiplication/MatrixA.seq

% hama seqdumper \
 -seqFile /examples/input/matrixmultiplication/MatrixB_transposed.seq

% hadoop fs -cat /examples/output/matrixmultiplication/part-00000

# Matrix A
#    9     4     1     9
#    1     8     6     3
#    8     3     3     9
#    7     1     9     6

# Matrix B (not transposed)
#    2     1     6     5
#    7     8     9     5
#    2     1     5     8
#    7     4     4     9

# Resulting Matrix C
#  111.0 78.0 131.0 154.0
#   91.0 83.0 120.0 120.0
#  106.0 71.0 126.0 160.0
#   81.0 48.0 120.0 166.0

% hadoop fs -rmr /examples/output/matrixmultiplication

####################################################################

