/**
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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUnaryUDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.io.IntWritable;


/**
 * Implement vectorized function conv(string, int,  int) returning string.
 * Support for use on numbers instead of strings shall be implemented
 * by inserting an explicit cast to string. There will not be VectorExpression
 * classes specifically for conv applied to numbers.
 */
public class StringConv extends StringUnaryUDF {
  private static final long serialVersionUID = 1L;

  StringConv(int colNum, int outputColumn, int fromBase, int toBase) {
    super(colNum, outputColumn, (IUDFUnaryString) new ConvWrapper(fromBase, toBase));
  }

  StringConv() {
    super();
  }

  /* This wrapper class implements the evaluate() method expected
   * by the superclass for use in the inner loop of vectorized expression
   * evaluation. It holds the fromBase and toBase arguments to
   * make the interface simply "Text evaluate(Text)" as expected.
   */
  static class ConvWrapper implements IUDFUnaryString {
    UDFConv conv;
    IntWritable fromBase;
    IntWritable toBase;

    ConvWrapper(int fromBase, int toBase) {
      conv = new UDFConv();
      this.fromBase = new IntWritable(fromBase);
      this.toBase = new IntWritable(toBase);
    }

    @Override
    public Text evaluate(Text s) {
      return conv.evaluate(s, fromBase, toBase);
    }
  }
}
