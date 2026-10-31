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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.serde2.objectinspector;

/**
 * ConstantObjectInspector for Iceberg V3 unknown type columns.
 * Unknown values are always null.
 */
public final class UnknownConstantObjectInspector implements ConstantObjectInspector {

  private static final UnknownConstantObjectInspector INSTANCE = new UnknownConstantObjectInspector();

  private UnknownConstantObjectInspector() {
  }

  public static UnknownConstantObjectInspector get() {
    return INSTANCE;
  }

  @Override
  public Category getCategory() {
    return Category.UNKNOWN;
  }

  @Override
  public String getTypeName() {
    return UnknownObjectInspector.get().getTypeName();
  }

  @Override
  public Object getWritableConstantValue() {
    return null;
  }
}
