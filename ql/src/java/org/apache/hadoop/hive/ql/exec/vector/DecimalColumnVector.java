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

package org.apache.hadoop.hive.ql.exec.vector;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.Writable;

public class DecimalColumnVector extends ColumnVector {

  /**
   * A vector if Decimal128 objects. These are mutable and have fairly
   * efficient operations on them. This will make it faster to load
   * column vectors and perform decimal vector operations with decimal-
   * specific VectorExpressions.
   *
   * For high performance and easy access to this low-level structure,
   * the fields are public by design (as they are in other ColumnVector
   * types).
   */
  public Decimal128[] vector;
  public short scale;
  public short precision;

  public DecimalColumnVector(int precision, int scale) {
    super(VectorizedRowBatch.DEFAULT_SIZE);
    this.precision = (short) precision;
    this.scale = (short) scale;
    final int len = VectorizedRowBatch.DEFAULT_SIZE;
    vector = new Decimal128[len];
    for (int i = 0; i < len; i++) {
      vector[i] = new Decimal128(0, this.scale);
    }
  }

  @Override
  public Writable getWritableObject(int index) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    // TODO Auto-generated method stub
  }
}
