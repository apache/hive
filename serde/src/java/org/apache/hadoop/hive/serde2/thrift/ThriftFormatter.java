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
package org.apache.hadoop.hive.serde2.thrift;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.FetchFormatter;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ThriftFormatter implements FetchFormatter<Object> {

  int protocol;

  @Override
  public void initialize(Configuration hconf, Properties props) throws Exception {
    protocol = hconf.getInt(SerDeUtils.LIST_SINK_OUTPUT_PROTOCOL, 0);
  }

  @Override
  public Object convert(Object row, ObjectInspector rowOI) throws Exception {
    StructObjectInspector structOI = (StructObjectInspector) rowOI;
    List<? extends StructField> fields = structOI.getAllStructFieldRefs();
    Object[] converted = new Object[fields.size()];
    for (int i = 0 ; i < converted.length; i++) {
      StructField fieldRef = fields.get(i);
      Object field = structOI.getStructFieldData(row, fieldRef);
      converted[i] = field == null ? null :
          SerDeUtils.toThriftPayload(field, fieldRef.getFieldObjectInspector(), protocol);
    }
    return converted;
  }

  @Override
  public void close() throws IOException {
  }
}