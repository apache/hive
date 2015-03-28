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

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.serde2.io.HiveIntervalDayTimeWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class JavaHiveIntervalDayTimeObjectInspector
    extends AbstractPrimitiveJavaObjectInspector
    implements SettableHiveIntervalDayTimeObjectInspector{

  public JavaHiveIntervalDayTimeObjectInspector() {
    super(TypeInfoFactory.intervalDayTimeTypeInfo);
  }

  @Override
  public HiveIntervalDayTime getPrimitiveJavaObject(Object o) {
    return o == null ? null : (HiveIntervalDayTime) o;
  }

  @Override
  public HiveIntervalDayTimeWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : new HiveIntervalDayTimeWritable((HiveIntervalDayTime) o);
  }

  @Override
  public Object set(Object o, HiveIntervalDayTime i) {
    return i == null ? null : new HiveIntervalDayTime(i);
  }

  @Override
  public Object set(Object o, HiveIntervalDayTimeWritable i) {
    return i == null ? null : i.getHiveIntervalDayTime();
  }

  @Override
  public Object create(HiveIntervalDayTime i) {
    return i == null ? null : new HiveIntervalDayTime(i);
  }
}
