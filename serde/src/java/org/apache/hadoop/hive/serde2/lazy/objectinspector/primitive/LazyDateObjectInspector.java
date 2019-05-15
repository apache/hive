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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * A WritableDateObjectInspector inspects a DateWritableV2 Object.
 */
public class LazyDateObjectInspector
    extends AbstractPrimitiveLazyObjectInspector<DateWritableV2>
    implements DateObjectInspector {

  protected LazyDateObjectInspector() {
    super(TypeInfoFactory.dateTypeInfo);
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new LazyDate((LazyDate) o);
  }

  @Override
  public Date getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((LazyDate) o).getWritableObject().get();
  }
}
