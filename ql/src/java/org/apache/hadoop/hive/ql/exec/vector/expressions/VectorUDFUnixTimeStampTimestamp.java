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

import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;

/**
 * Return Unix Timestamp.
 * Extends {@link VectorUDFTimestampFieldTimestamp}
 */
public final class VectorUDFUnixTimeStampTimestamp extends VectorUDFTimestampFieldTimestamp {

  private static final long serialVersionUID = 1L;

  @Override
  protected long getTimestampField(TimestampColumnVector timestampColVector, int elementNum) {
    return timestampColVector.asScratchTimestamp(elementNum).getTime() / 1000;
  }

  public VectorUDFUnixTimeStampTimestamp(int colNum, int outputColumnNum) {
    /* not a real field */
    super(-1, colNum, outputColumnNum);
  }

  public VectorUDFUnixTimeStampTimestamp() {
    super();
  }

}
