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
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hive.common.util.DateParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Date;

/**
 * Vectorized version of TO_DATE(STRING)
 * As TO_DATE() now returns DATE type, this should be the same behavior as the DATE cast operator.
 */
public class VectorUDFDateString extends CastStringToDate {
  private static final long serialVersionUID = 1L;

  public VectorUDFDateString() {

  }

  public VectorUDFDateString(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
  }
}
