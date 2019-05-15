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
package org.apache.hadoop.hive.ql.log.syslog;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class SyslogSerDe extends AbstractSerDe {
  private static final String COLUMN_NAMES =
    "facility,severity,version,ts,hostname,app_name,proc_id,msg_id,structured_data,msg,unmatched";
  private static final String COLUMN_NAME_DELIMITER = String.valueOf(SerDeUtils.COMMA);
  private static final String COLUMN_TYPES =
    "string:string:string:timestamp:string:string:string:string:map<string,string>:binary:binary";

  private ObjectInspector inspector;
  private SyslogParser syslogParser;
  private List<Object> EMPTY_ROW;

  @Override
  public void initialize(@Nullable final Configuration configuration, final Properties properties)
    throws SerDeException {

    final List<String> columnNames = Arrays.asList(COLUMN_NAMES.split(COLUMN_NAME_DELIMITER));
    final List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(COLUMN_TYPES);

    EMPTY_ROW = new ArrayList<>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      EMPTY_ROW.add(null);
    }
    StructTypeInfo typeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    this.inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
    syslogParser = new SyslogParser();
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Writable serialize(final Object o, final ObjectInspector objectInspector) throws SerDeException {
    throw new SerDeException("Serialization is not supported yet");
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Object deserialize(final Writable writable) throws SerDeException {
    Text rowText = (Text) writable;
    if (rowText.getLength() == 0) {
      // add the empty byte[] as unmatched line
      EMPTY_ROW.set(EMPTY_ROW.size() - 1, rowText.getBytes());
      return EMPTY_ROW;
    }
    ByteArrayInputStream bis = new ByteArrayInputStream(rowText.getBytes(), 0, rowText.getLength());
    syslogParser.setInputStream(bis);
    try {
      return syslogParser.readEvent();
    } catch (Exception e) {
      throw new SerDeException("Failed parsing line: " + rowText.toString(), e);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return inspector;
  }
}
