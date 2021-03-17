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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;
import org.apache.hive.common.util.DateParser;

/**
 * Vectorized implementation of trunc(date, fmt) function for string input
 */
public class TruncDateFromString extends TruncDateFromTimestamp {
  private transient Date date = new Date();

  public TruncDateFromString(int colNum, byte[] fmt, int outputColumnNum) {
    super(colNum, fmt, outputColumnNum);
  }

  private static final long serialVersionUID = 1L;

  public TruncDateFromString() {
    super();
  }

  protected void truncDate(ColumnVector inV, BytesColumnVector outV, int i) {
    truncDate((BytesColumnVector) inV, outV, i);
  }

  protected void truncDate(BytesColumnVector inV, BytesColumnVector outV, int i) {
    if (inV.vector[i] == null) {
      outV.isNull[i] = true;
      outV.noNulls = false;
    }

    String dateString =
        new String(inV.vector[i], inV.start[i], inV.length[i], StandardCharsets.UTF_8);
    if (DateParser.parseDate(dateString, date)) {
      processDate(outV, i, date);
    } else {
      outV.isNull[i] = true;
      outV.noNulls = false;
    }
  }

  @Override
  protected ArgumentType getInputColumnType() {
    return VectorExpressionDescriptor.ArgumentType.STRING_FAMILY;
  }
}
