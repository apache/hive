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
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.serde2.io.DateWritable;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.text.SimpleDateFormat;

public class VectorUDFDateLong extends LongToStringUnaryUDF {
  private static final long serialVersionUID = 1L;

  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private transient Date date = new Date(0);

  public VectorUDFDateLong() {
    super();
  }

  public VectorUDFDateLong(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
  }

  @Override
  protected void func(BytesColumnVector outV, long[] vector, int i) {
    switch (inputTypes[0]) {
      case DATE:
        date.setTime(DateWritable.daysToMillis((int) vector[i]));
        break;

      case TIMESTAMP:
        date.setTime(vector[i] / 1000000);
        break;
      default:
        throw new Error("Unsupported input type " + inputTypes[0].name());
    }
    try {
      byte[] bytes = formatter.format(date).getBytes("UTF-8");
      outV.setRef(i, bytes, 0, bytes.length);
    } catch (UnsupportedEncodingException e) {
      outV.vector[i] = null;
      outV.isNull[i] = true;
    }
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(1)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.DATETIME_FAMILY)
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.COLUMN);
    return b.build();
  }
}
