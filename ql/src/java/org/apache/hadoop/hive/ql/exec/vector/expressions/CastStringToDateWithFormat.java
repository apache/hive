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

import org.apache.hadoop.hive.common.format.datetime.HiveSqlDateTimeFormatter;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;

import java.nio.charset.StandardCharsets;

/**
 * Vectorized UDF for CAST (&lt;STRING&gt; TO DATE WITH FORMAT &lt;STRING&gt;).
 */
public class CastStringToDateWithFormat extends CastStringToDate {

  private static final long serialVersionUID = 1L;
  private HiveSqlDateTimeFormatter formatter;

  public CastStringToDateWithFormat() {
    super();
  }

  public CastStringToDateWithFormat(int inputColumn, byte[] patternBytes, int outputColumnNum) {
    super(inputColumn, outputColumnNum);

    if (patternBytes == null) {
      throw new IllegalStateException(
          "Tried to cast (<string> to date with format <pattern>)," + " but <pattern> not found");
    }
    formatter =
        new HiveSqlDateTimeFormatter(new String(patternBytes, StandardCharsets.UTF_8), true);
  }

  @Override protected void evaluate(LongColumnVector outputColVector, BytesColumnVector inV,
      int i) {
    String dateString =
        new String(inV.vector[i], inV.start[i], inV.length[i], StandardCharsets.UTF_8);
    Date date = formatter.parseDate(dateString.replaceAll("\u0000", ""));
    if (date != null) {
      outputColVector.vector[i] = DateWritableV2.dateToDays(date);
    } else {
      super.setNull(outputColVector, i);
    }
  }
}
