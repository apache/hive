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

package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hive.serde2.BaseStructObjectInspector;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleHBaseKeyFactory extends DefaultHBaseKeyFactory {

  private static final String DELIMITER_PATTERN = "\\$\\$";
  private static final byte[] DELIMITER_BINARY = "$$".getBytes();

  @Override
  public ObjectInspector createKeyObjectInspector(TypeInfo type) {
    return new SlashSeparatedOI((StructTypeInfo)type);
  }

  @Override
  public LazyObjectBase createKey(ObjectInspector inspector) throws SerDeException {
    return new DoubleDollarSeparated();
  }

  private final ByteStream.Output output = new ByteStream.Output();

  @Override
  public byte[] serializeKey(Object object, StructField field) throws IOException {
    ObjectInspector inspector = field.getFieldObjectInspector();
    if (inspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new IllegalStateException("invalid type value " + inspector.getTypeName());
    }
    output.reset();
    for (Object element : ((StructObjectInspector)inspector).getStructFieldsDataAsList(object)) {
      if (output.getLength() > 0) {
        output.write(DELIMITER_BINARY);
      }
      output.write(String.valueOf(element).getBytes());
    }
    return output.getLength() > 0 ? output.toByteArray() : null;
  }

  private static class DoubleDollarSeparated implements LazyObjectBase {

    private Object[] fields;
    private transient boolean isNull;

    @Override
    public void init(ByteArrayRef bytes, int start, int length) {
      fields = new String(bytes.getData(), start, length).split(DELIMITER_PATTERN);
      isNull = false;
    }

    @Override
    public void setNull() {
      isNull = true;
    }

    @Override
    public Object getObject() {
      return isNull ? null : this;
    }
  }

  private static class SlashSeparatedOI extends BaseStructObjectInspector {

    private int length;

    private SlashSeparatedOI(StructTypeInfo type) {
      List<String> names = type.getAllStructFieldNames();
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      for (int i = 0; i < names.size(); i++) {
        ois.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      }
      init(names, ois, null);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
      return ((DoubleDollarSeparated)data).fields[((MyField)fieldRef).getFieldID()];
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
      return Arrays.asList(((DoubleDollarSeparated)data).fields);
    }
  }
}
