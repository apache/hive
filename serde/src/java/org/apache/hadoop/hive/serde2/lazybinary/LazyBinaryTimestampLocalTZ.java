/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.lazybinary;

import java.time.ZoneId;

import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableTimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;

public class LazyBinaryTimestampLocalTZ extends
    LazyBinaryPrimitive<WritableTimestampLocalTZObjectInspector, TimestampLocalTZWritable> {

  private ZoneId timeZone;

  public LazyBinaryTimestampLocalTZ(WritableTimestampLocalTZObjectInspector oi) {
    super(oi);
    TimestampLocalTZTypeInfo typeInfo = (TimestampLocalTZTypeInfo) oi.getTypeInfo();
    this.timeZone = typeInfo.timeZone();
    this.data = new TimestampLocalTZWritable();
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    data.set(bytes.getData(), start, timeZone);
  }
}
