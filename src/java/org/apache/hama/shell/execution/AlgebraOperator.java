/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.shell.execution;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hama.Matrix;

public class AlgebraOperator {

  // to store Matrix, AlgebraOperator.
  // if *operand* is an AlgebraOperator,
  // we should call AlgebraOperator.getValue()
  // to get actual value, then do algebraic operation.
  Object operand;
  List<Entry> otherOperands;

  /**
   * 
   * An Entry Class to store all the Operators and other Operands.
   * 
   */
  static class Entry {
    String op;
    AlgebraOperator operand;

    public Entry(String op, AlgebraOperator operand) {
      this.op = op;
      this.operand = operand;
    }

    public String getOp() {
      return this.op;
    }

    public AlgebraOperator getOperand() {
      return this.operand;
    }
  }

  public AlgebraOperator(Object operand) {
    this.operand = operand;
    this.otherOperands = null;
  }

  public void addOpAndOperand(String op, AlgebraOperator otherOperand) {
    if (otherOperands == null) {
      this.otherOperands = new ArrayList<Entry>();
    }
    this.otherOperands.add(new Entry(op, otherOperand));
  }

  /**
   * Do the actual algebra operation here.
   * 
   * if it is a
   * 
   * @see Matrix operator, return it's value (matrix). if it is a
   * @see Matrix#add(Matrix) operator, do addition and return it's value
   *      (matrix). if it is a
   * @see Matrix#mult(Matrix) operator, do multiplication and return it's value
   *      (matrix).
   * 
   * @return Matrix ( or AlgebraOperator(used internally) )
   * @throws AlgebraOpException
   * @throws IOException
   */
  public Object getValue() throws AlgebraOpException, IOException {
    // A Unary Operator.
    //
    // A Matrix or A Number, we just return it.
    //
    // An AlgebraOperator
    // we return value through AlgebraOperator.getValue()
    if (this.otherOperands == null) {
      if (operand == null)
        throw new AlgebraOpException("An AlgebraOperator can not be null. "
            + "Please check the expression.");
      else {
        if (this.operand instanceof AlgebraOperator)
          return ((AlgebraOperator) this.operand).getValue();
        else
          return this.operand;
      }
    }
    // An AlgebraOperation.
    else {
      if (operand == null)
        throw new AlgebraOpException("An AlgebraOperator can not be null. "
            + "Please check the expression.");

      // first, if *operand* is AlgebraOperator,
      // turn *operand* to value-style(Matrix, Number)
      if (this.operand instanceof AlgebraOperator)
        this.operand = ((AlgebraOperator) this.operand).getValue();

      assert this.otherOperands.size() != 0 : "Other operands in an algebra operation should not be zero while [*otherOperands*!=null]";

      // collect all the matrices.
      List<Matrix> matrices = new ArrayList<Matrix>();

      // we should check the Operator belongs to "+"/"-" or "*".

      // "*"

      if (this.otherOperands.get(0).getOp().equalsIgnoreCase("*")) {

        // we check the operand first.
        if (this.operand instanceof Matrix) {
          matrices.add((Matrix) this.operand);
        } else
          throw new AlgebraOpException("Uncognized Object Type.");

        // now we loop the otherOperands list.
        for (Entry entry : this.otherOperands) {
          Object value = entry.getOperand().getValue();

          if (value instanceof Matrix) {
            matrices.add((Matrix) value);
          } else
            throw new AlgebraOpException("Uncognized Object Type.");
        }

        // Now do the multiplication.
        return doListMultiplication(matrices);
      }
      // "+"/"-"
      // 
      // We only accept the below expression.
      //
      // Matrix (+/-) Matrix
      else {
        // collect the ops;
        List<String> ops = new ArrayList<String>();
        // make sure that all the Matrices have same rows and columns.
        // for example: A + B + C + D + ... + Z
        // we will check:
        // A.rows = B.rows = C.rows = D.rows = ... = Z.rows
        // A.columns = B.columns = C.columns = D.columns = ... = Z.columns
        Matrix m = (Matrix) this.operand;
        int rows = m.getRows();
        int columns = m.getColumns();
        for (Entry entry : this.otherOperands) {
          Object value = entry.getOperand().getValue();
          String keyOp = entry.getOp();

          if (value instanceof Matrix) {
            matrices.add((Matrix) value);
            ops.add(keyOp);
          } else
            throw new AlgebraOpException("Uncognized Object Type.");

          if (rows != ((Matrix) value).getRows()
              && columns != ((Matrix) value).getColumns())
            throw new AlgebraOpException(
                "Matrices' rows and columns should be same while A + B + ... + Z. Please check the expression.");
        }

        m = (Matrix) this.operand;
        for (int i = 0; i < ops.size(); i++) {

          if (ops.get(i).equalsIgnoreCase("+")) {
            m = m.add(matrices.get(i));
          } else if (ops.get(i).equalsIgnoreCase("-")) {
            m = m.add(-1, matrices.get(i));
          } else
            throw new AlgebraOpException(
                "Uncognized Operator. It should be '+' '-' '*'.");

        }

        return m;

      }
    } // End of AlgebraOperation.

  }

  private Matrix doListMultiplication(List<Matrix> matrices)
      throws IOException, AlgebraOpException {

    // do Matrices Multiplication.
    // First make sure that all the Matrices are suitable for multiplications.
    // for example: A * B * C * D * ... * Z
    // we will check A.columns = B.rows, B.columns = C.rows, C.columns = D.rows
    // ...

    for (int i = 0, j = 1; j < matrices.size(); i++, j++) {
      if (matrices.get(i).getColumns() != matrices.get(j).getRows())
        throw new AlgebraOpException(
            "A's columns should equal with B's rows while A * B. Please check the expression.");
    }

    // Now we can do multiplication.
    Matrix matrix = matrices.get(0);
    for (int i = 1; i < matrices.size(); i++) {
      Matrix m = matrices.get(i);
      matrix = matrix.mult(m);
    }

    return matrix;
  }

}
