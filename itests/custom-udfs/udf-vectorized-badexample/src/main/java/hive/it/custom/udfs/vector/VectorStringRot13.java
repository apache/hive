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

package hive.it.custom.udfs.vector;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUnaryUDF;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUnaryUDFDirect;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.orc.impl.TreeReaderFactory.BytesColumnVectorUtil;

public class VectorStringRot13 extends StringUnaryUDFDirect {

  public VectorStringRot13(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
  }
  
  public VectorStringRot13() {
    super();
  }

  @Override
  protected void func(BytesColumnVector outV, byte[][] vector, int[] start,
      int[] length, int i) {
    int off = start[i];
    int len = length[i];
    byte[] src = vector[i];
    byte[] dst = new byte[len];
    for (int j = 0; j < len ; j++) {
      dst[j] = rot13(src[off+j]);
    }
    outV.setVal(i, dst, 0, length[i]);
  }

  private byte rot13(byte b) {
    if (b >= 'a' && b <= 'm' || b >= 'A' && b <= 'M' ) {
      return (byte) (b+13);
    }
    if (b >= 'n' && b <= 'z' || b >= 'N' && b <= 'Z') {
      return (byte) (b-13);
    }
    return b;
    }
}
