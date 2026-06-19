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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;

/**
 * A WritableConstantHiveCharObjectInspector is a WritableHiveCharObjectInspector
 * that implements ConstantObjectInspector.
 */
public class WritableConstantHiveCharObjectInspector extends
    WritableHiveCharObjectInspector implements
    ConstantObjectInspector {

  protected HiveCharWritable value;

  // no-arg ctor required for Kyro serialization
  WritableConstantHiveCharObjectInspector() {
  }

  WritableConstantHiveCharObjectInspector(CharTypeInfo typeInfo,
      HiveCharWritable value) {
    super(typeInfo);
    this.value = value;
  }

  @Override
  public HiveCharWritable getWritableConstantValue() {
    return value;
  }
}
