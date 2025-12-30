/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.serde.objectinspector;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampNano;
import org.apache.hadoop.hive.serde2.io.TimestampNanoWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class IcebergNanosecondTimestampObjectInspectorHive3 extends IcebergTimestampObjectInspectorHive3 {

  private static final IcebergNanosecondTimestampObjectInspectorHive3 INSTANCE =
      new IcebergNanosecondTimestampObjectInspectorHive3();

  public static IcebergNanosecondTimestampObjectInspectorHive3 get() {
    return INSTANCE;
  }

  private IcebergNanosecondTimestampObjectInspectorHive3() {
    super(TypeInfoFactory.timestampNanoTypeInfo);
  }

  @Override
  public TimestampNano getPrimitiveJavaObject(Object o) {
    Timestamp timestamp = super.getPrimitiveJavaObject(o);
    if (timestamp == null) {
      return null;
    }
    return new TimestampNano(timestamp);
  }

  @Override
  public TimestampNanoWritable getPrimitiveWritableObject(Object o) {
    TimestampNano ts = getPrimitiveJavaObject(o);
    return ts == null ? null : new TimestampNanoWritable(ts);
  }
}
