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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;

import java.nio.charset.StandardCharsets;

/**
 * Vectorized UDF for CAST (&lt;DATE&gt; TO VARCHAR(&lt;LENGTH&gt;) WITH FORMAT &lt;STRING&gt;).
 */
public class CastDateToVarCharWithFormat extends CastDateToVarChar {

  private static final long serialVersionUID = 1L;
  private HiveSqlDateTimeFormatter sqlFormatter;

  public CastDateToVarCharWithFormat() {
    super();
  }

  public CastDateToVarCharWithFormat(int inputColumn, byte[] patternBytes, int len,
      int outputColumnNum) {
    super(inputColumn, outputColumnNum);

    if (patternBytes == null) {
      throw new IllegalStateException(
          "Tried to cast (<date> to varchar with format <pattern>)," + " but <pattern> not found");
    }
    sqlFormatter =
        new HiveSqlDateTimeFormatter(new String(patternBytes, StandardCharsets.UTF_8), false);
  }

  @Override protected void func(BytesColumnVector outV, long[] vector, int i) {
    super.sqlFormat(outV, vector, i, sqlFormatter);
  }

  @Override public String vectorExpressionParameters() {
    return super.vectorExpressionParameters() + ", format pattern: " + sqlFormatter.getPattern();
  }
}
