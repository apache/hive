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
package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;

import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * A WritableBooleanObjectInspector inspects a BooleanWritable Object.
 */
public class LazyBooleanObjectInspector extends
    AbstractPrimitiveLazyObjectInspector<BooleanWritable> implements
    BooleanObjectInspector {
  // Whether characters, such as 't/T', 'f/F', and '1/0' are interpreted as valid boolean literals.
  private boolean extendedLiteral = false;

  LazyBooleanObjectInspector() {
    super(TypeInfoFactory.booleanTypeInfo);
  }

  @Override
  public boolean get(Object o) {
    return getPrimitiveWritableObject(o).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new LazyBoolean((LazyBoolean) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    return o == null ? null : Boolean.valueOf(get(o));
  }

  public boolean isExtendedLiteral() {
    return extendedLiteral;
  }

  public void setExtendedLiteral(boolean extendedLiteral) {
    this.extendedLiteral = extendedLiteral;
  }

}
