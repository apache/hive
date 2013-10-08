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

import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;

/**
 * An AbstractPrimitiveLazyObjectInspector for a LazyPrimitive object.
 */
public abstract class AbstractPrimitiveLazyObjectInspector<T extends Writable>
    extends AbstractPrimitiveObjectInspector {

  protected AbstractPrimitiveLazyObjectInspector() {
    super();
  }
  protected AbstractPrimitiveLazyObjectInspector(PrimitiveTypeInfo typeInfo) {
    super(typeInfo);
  }

  @Override
  public T getPrimitiveWritableObject(Object o) {
    return o == null ? null : ((LazyPrimitive<?, T>) o).getWritableObject();
  }

  @Override
  public boolean preferWritable() {
    return true;
  }

}
