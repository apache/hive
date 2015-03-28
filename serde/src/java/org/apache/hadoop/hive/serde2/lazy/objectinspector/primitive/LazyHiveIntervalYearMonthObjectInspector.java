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

import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.io.HiveIntervalYearMonthWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveIntervalYearMonth;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveIntervalYearMonthObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class LazyHiveIntervalYearMonthObjectInspector
    extends AbstractPrimitiveLazyObjectInspector<HiveIntervalYearMonthWritable>
    implements HiveIntervalYearMonthObjectInspector{

  LazyHiveIntervalYearMonthObjectInspector() {
    super(TypeInfoFactory.intervalYearMonthTypeInfo);
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new LazyHiveIntervalYearMonth((LazyHiveIntervalYearMonth) o);
  }

  @Override
  public HiveIntervalYearMonth getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((LazyHiveIntervalYearMonth) o).getWritableObject().getHiveIntervalYearMonth();
  }
}
