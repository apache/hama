####################################################################
# Hama Pipes README                                                #
####################################################################
# Hama Pipes includes the following three examples:                #
# - 1) Summation                                                   #
# - 2) PiEstimator                                                 #
# - 3) MatrixMultiplication                                        #
# in c++/src/main/native/examples                                  #
# Please see c++/src/main/native/examples/README.txt               #
####################################################################

Please use the following command to compile:

% g++ -m64 -Ic++/src/main/native/utils/api \
      -Ic++/src/main/native/pipes/api \
      -Lc++/target/native \
      -lhadooputils -lpthread \
      PROGRAM.cc \
      -o PROGRAM \
      -g -Wall -O2

Attention: The paths have to be adjusted, if you are not operating
           in the Hama source folder.

####################################################################
