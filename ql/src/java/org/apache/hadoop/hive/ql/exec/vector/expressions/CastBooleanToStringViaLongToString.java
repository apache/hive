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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;

public class CastBooleanToStringViaLongToString extends LongToStringUnaryUDF {
  private static final long serialVersionUID = 1L;
  private transient byte[] temp; // space to put date string
  private static final byte[][] dictionary = { {'F', 'A', 'L', 'S', 'E'}, {'T', 'R', 'U', 'E'} };

  public CastBooleanToStringViaLongToString() {
    super();
    temp = new byte[8];
  }

  public CastBooleanToStringViaLongToString(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
    temp = new byte[8];
  }

  @Override
  protected void func(BytesColumnVector outV, long[] vector, int i) {

    /* 0 is false and 1 is true in the input vector, so a simple dictionary is used
     * with two entries. 0 references FALSE and 1 references TRUE in the dictionary.
     */
    outV.setVal(i, dictionary[(int) vector[i]], 0, dictionary[(int) vector[i]].length);
  }
}
