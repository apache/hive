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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.math.BigDecimal;

import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 * A WritableConstantLongObjectInspector is a WritableLongObjectInspector
 * that implements ConstantObjectInspector.
 */
public class WritableConstantLongObjectInspector extends
    WritableLongObjectInspector implements
    ConstantObjectInspector {

  private LongWritable value;

  protected WritableConstantLongObjectInspector() {
    super();
  }

  WritableConstantLongObjectInspector(LongWritable value) {
    super();
    this.value = value;
  }

  @Override
  public LongWritable getWritableConstantValue() {
    return value;
  }

  @Override
  public int precision() {
    return BigDecimal.valueOf(value.get()).precision();
  }

}
