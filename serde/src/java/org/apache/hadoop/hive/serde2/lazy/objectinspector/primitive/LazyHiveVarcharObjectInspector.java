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
package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;


import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.BaseTypeParams;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeParams;
import org.apache.hadoop.hive.serde2.typeinfo.ParameterizedPrimitiveTypeUtils;

public class LazyHiveVarcharObjectInspector
    extends AbstractPrimitiveLazyObjectInspector<HiveVarcharWritable>
    implements HiveVarcharObjectInspector {

  public LazyHiveVarcharObjectInspector(PrimitiveTypeEntry typeEntry) {
    super(typeEntry);
    if (typeEntry.primitiveCategory != PrimitiveCategory.VARCHAR) {
      throw new RuntimeException(
          "TypeEntry of type varchar expected, got " + typeEntry.primitiveCategory);
    }
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    LazyHiveVarchar ret = new LazyHiveVarchar(this);
    ret.setValue((LazyHiveVarchar) o);
    return ret;
  }

  @Override
  public HiveVarchar getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }

    HiveVarchar ret = ((LazyHiveVarchar) o).getWritableObject().getHiveVarchar();
    if (!ParameterizedPrimitiveTypeUtils.doesPrimitiveMatchTypeParams(
        ret, (VarcharTypeParams) typeParams)) {
      HiveVarchar newValue = new HiveVarchar(ret, ((VarcharTypeParams) typeParams).length);
      return newValue;
    }
    return ret;
  }

  public String toString() {
    return getTypeName();
  }
}
