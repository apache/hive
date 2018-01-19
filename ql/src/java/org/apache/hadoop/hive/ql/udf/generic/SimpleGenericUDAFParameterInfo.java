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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * A simple implementation of <tt>GenericUDAFParameterInfo</tt>.
 *
 */
public class SimpleGenericUDAFParameterInfo implements GenericUDAFParameterInfo
{

  private final ObjectInspector[] parameters;
  private final boolean isWindowing;
  private final boolean distinct;
  private final boolean allColumns;

  public SimpleGenericUDAFParameterInfo(ObjectInspector[] params, boolean isWindowing, boolean distinct,
      boolean allColumns) {
    this.parameters = params;
    this.isWindowing = isWindowing;
    this.distinct = distinct;
    this.allColumns = allColumns;
  }

  @Override
  @Deprecated
  public TypeInfo[] getParameters() {
    TypeInfo[] result = new TypeInfo[parameters.length];
    for (int ii = 0; ii < parameters.length; ++ii) {
      result[ii] =
        TypeInfoUtils.getTypeInfoFromObjectInspector(parameters[ii]);
    }
    return result;
  }

  public ObjectInspector[] getParameterObjectInspectors() {
    return parameters;
  }

  @Override
  public boolean isDistinct() {
    return distinct;
  }

  @Override
  public boolean isAllColumns() {
    return allColumns;
  }

  @Override
  public boolean isWindowing() {
    return isWindowing;
  }
}
