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

public class WritableHiveIntervalDayTimeObjectInspector
    extends AbstractPrimitiveWritableObjectInspector
    implements SettableHiveIntervalDayTimeObjectInspector{

  public WritableHiveIntervalDayTimeObjectInspector() {
    super(TypeInfoFactory.intervalDayTimeTypeInfo);
  }

  @Override
  public HiveIntervalDayTime getPrimitiveJavaObject(Object o) {
    return o == null ? null : ((HiveIntervalDayTimeWritable) o).getHiveIntervalDayTime();
  }

  @Override
  public HiveIntervalDayTimeWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : (HiveIntervalDayTimeWritable) o;
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new HiveIntervalDayTimeWritable((HiveIntervalDayTimeWritable) o);
  }

  @Override
  public Object set(Object o, HiveIntervalDayTime i) {
    if (i == null) {
      return null;
    }
    ((HiveIntervalDayTimeWritable) o).set(i);
    return o;
  }

  @Override
  public Object set(Object o, HiveIntervalDayTimeWritable i) {
    if (i == null) {
      return null;
    }
    ((HiveIntervalDayTimeWritable) o).set(i);
    return o;
  }

  @Override
  public Object create(HiveIntervalDayTime i) {
    return i == null ? null : new HiveIntervalDayTimeWritable(i);
  }
}
