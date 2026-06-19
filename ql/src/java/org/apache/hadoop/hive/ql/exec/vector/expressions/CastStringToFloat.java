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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;

/**
 * Cast a string to a double.
 *
 * If other functions besides cast need to take a string in and produce a long,
 * you can subclass this class or convert it to a superclass, and
 * implement different "func()" methods for each operation.
 */
public class CastStringToFloat extends CastStringToDouble {
  private static final long serialVersionUID = 1L;

  public CastStringToFloat(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public CastStringToFloat() {
    super();
  }

  /**
   * Convert input string to a double, at position i in the respective vectors.
   */
  @Override
  protected void func(DoubleColumnVector outV, BytesColumnVector inV, int batchIndex) {

    byte[] bytes = inV.vector[batchIndex];
    final int start = inV.start[batchIndex];
    final int length = inV.length[batchIndex];
    try {
      if (!LazyUtils.isNumberMaybe(bytes, start, length)) {
        outV.noNulls = false;
        outV.isNull[batchIndex] = true;
        outV.vector[batchIndex] = DoubleColumnVector.NULL_VALUE;
        return;
      }
      outV.vector[batchIndex] =
          Float.parseFloat(
              new String(bytes, start, length, StandardCharsets.UTF_8));
    } catch (Exception e) {

      // for any exception in conversion to integer, produce NULL
      outV.noNulls = false;
      outV.isNull[batchIndex] = true;
      outV.vector[batchIndex] = DoubleColumnVector.NULL_VALUE;
    }
  }
}