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


import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;

/**
 * Cast a string to a decimal64.
 *
 * If other functions besides cast need to take a string in and produce a decimal64,
 * you can subclass this class or convert it to a superclass, and
 * implement different "func()" methods for each operation.
 */
public class CastStringToDecimal64 extends CastStringToDecimal {
  private static final long serialVersionUID = 1L;

  public CastStringToDecimal64(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public CastStringToDecimal64() {
    super();
  }
  
  @Override
  protected void setOutputColumnVectorValue(ColumnVector outputColVector, int i, String s) {
    ((Decimal64ColumnVector) outputColVector).set(i, HiveDecimal.create(s));
  }
}