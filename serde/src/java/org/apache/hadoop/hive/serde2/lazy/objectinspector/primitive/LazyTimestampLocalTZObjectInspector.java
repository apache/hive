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
package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestampLocalTZ;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.apache.hive.common.util.TimestampParser;

import java.util.Collections;
import java.util.List;

public class LazyTimestampLocalTZObjectInspector
    extends AbstractPrimitiveLazyObjectInspector<TimestampLocalTZWritable>
    implements TimestampLocalTZObjectInspector {

  private final TimestampParser parser;

  protected LazyTimestampLocalTZObjectInspector(TimestampLocalTZTypeInfo typeInfo) {
    this(typeInfo, Collections.emptyList());
  }

  LazyTimestampLocalTZObjectInspector(TimestampLocalTZTypeInfo typeInfo, List<String> formats) {
    super(typeInfo);
    this.parser = new TimestampParser(formats == null ? Collections.emptyList() : formats);
  }

  @Override
  public TimestampTZ getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }

    TimestampTZ t = ((LazyTimestampLocalTZ) o).getWritableObject().getTimestampTZ();
    TimestampLocalTZTypeInfo timestampTZTypeInfo = (TimestampLocalTZTypeInfo) typeInfo;
    if (!t.getZonedDateTime().getZone().equals(timestampTZTypeInfo.timeZone())) {
      t.setZonedDateTime(t.getZonedDateTime().withZoneSameInstant(timestampTZTypeInfo.timeZone()));
    }
    return t;
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new LazyTimestampLocalTZ((LazyTimestampLocalTZ) o);
  }

  public TimestampParser getParser() {
    return parser;
  }
}
