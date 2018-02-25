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

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.logging.log4j.core.layout.StringBuilderEncoder;

/**
 * GenericUDFArray.
 *
 */
@Description(name = "cardinality_violation",
  value = "_FUNC_(n0, n1...) - raises Cardinality Violation")
public class GenericUDFCardinalityViolation extends GenericUDF {
  private transient Converter[] converters;
  private transient ArrayList<Object> ret = new ArrayList<Object>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    StringBuilder nonUniqueKey = new StringBuilder();
    for(DeferredObject t : arguments) {
      if(nonUniqueKey.length() > 0) {nonUniqueKey.append(','); }
      nonUniqueKey.append(t.get());
    }
    throw new RuntimeException("Cardinality Violation in Merge statement: " + nonUniqueKey);
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("cardinality_violation", children, ",");
  }
}
