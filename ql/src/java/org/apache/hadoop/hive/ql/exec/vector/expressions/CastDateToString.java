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
import org.apache.hadoop.hive.serde2.io.DateWritable;

import java.sql.Date;

public class CastDateToString extends LongToStringUnaryUDF {
  private static final long serialVersionUID = 1L;
  protected transient Date dt = new Date(0);

  public CastDateToString() {
    super();
  }

  public CastDateToString(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
  }

  @Override
  protected void func(BytesColumnVector outV, long[] vector, int i) {
    dt.setTime(DateWritable.daysToMillis((int) vector[i]));
    byte[] temp = dt.toString().getBytes();
    outV.setVal(i, temp, 0, temp.length);
  }
}
