/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.serde;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.accumulo.columns.ColumnEncoding;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.log4j.Logger;

/**
 * Example AccumuloRowIdFactory which accepts a delimiter that is used to separate the components of
 * some struct to place in the rowId.
 */
public class DelimitedAccumuloRowIdFactory extends DefaultAccumuloRowIdFactory {
  private static final Logger log = Logger.getLogger(DelimitedAccumuloRowIdFactory.class);
  public static final String ACCUMULO_COMPOSITE_DELIMITER = "accumulo.composite.delimiter";

  private byte separator;

  @Override
  public void init(AccumuloSerDeParameters accumuloSerDeParams, Properties properties)
      throws SerDeException {
    super.init(accumuloSerDeParams, properties);

    String delimiter = properties.getProperty(ACCUMULO_COMPOSITE_DELIMITER);
    if (null == delimiter || delimiter.isEmpty()) {
      throw new SerDeException("Did not find expected delimiter in configuration: "
          + ACCUMULO_COMPOSITE_DELIMITER);
    }

    if (delimiter.length() != 1) {
      log.warn("Configured delimiter is longer than one character, only using first character");
    }

    separator = (byte) delimiter.charAt(0);

    log.info("Initialized DelimitedAccumuloRowIdFactory with separator of '" + separator + "'");
  }

  @Override
  public ObjectInspector createRowIdObjectInspector(TypeInfo type) throws SerDeException {
    return LazyFactory.createLazyObjectInspector(type, new byte[] {separator}, 0,
        serdeParams.getNullSequence(), serdeParams.isEscaped(), serdeParams.getEscapeChar());
  }

  @Override
  public LazyObjectBase createRowId(ObjectInspector inspector) throws SerDeException {
    LazyObjectBase lazyObj = LazyFactory.createLazyObject(inspector,
        ColumnEncoding.BINARY == rowIdMapping.getEncoding());
    log.info("Created " + lazyObj.getClass() + " for rowId with inspector " + inspector.getClass());
    return lazyObj;
  }

  @Override
  public byte[] serializeRowId(Object object, StructField field, ByteStream.Output output)
      throws IOException {
    ObjectInspector inspector = field.getFieldObjectInspector();
    if (inspector.getCategory() != ObjectInspector.Category.STRUCT) {
      throw new IllegalStateException("invalid type value " + inspector.getTypeName());
    }

    output.reset();

    StructObjectInspector structOI = (StructObjectInspector) inspector;
    List<Object> elements = structOI.getStructFieldsDataAsList(object);
    List<? extends StructField> fields = structOI.getAllStructFieldRefs();
    for (int i = 0; i < elements.size(); i++) {
      Object o = elements.get(i);
      StructField structField = fields.get(i);

      if (output.getLength() > 0) {
        output.write(separator);
      }

      serializer.writeWithLevel(structField.getFieldObjectInspector(), o, output, rowIdMapping, 1);
    }

    return output.toByteArray();
  }
}
