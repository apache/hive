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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SchemaInference;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Writable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A serde class for ORC. It transparently passes the object to/from the ORC
 * file reader/writer. This SerDe does not support statistics, since serialized
 * size doesn't make sense in the context of ORC files.
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES, OrcSerde.COMPRESSION})
public class OrcSerde extends AbstractSerDe implements SchemaInference {
  private static final Logger LOG = LoggerFactory.getLogger(OrcSerde.class);

  private final OrcSerdeRow row = new OrcSerdeRow();
  private ObjectInspector inspector = null;

  static final String COMPRESSION = "orc.compress";
  static final Pattern UNQUOTED_NAMES = Pattern.compile("^[a-zA-Z0-9_]+$");

  final class OrcSerdeRow implements Writable {
    Object realRow;
    ObjectInspector inspector;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      throw new UnsupportedOperationException("can't write the bundle");
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      throw new UnsupportedOperationException("can't read the bundle");
    }

    ObjectInspector getInspector() {
      return inspector;
    }

    Object getRow() {
      return realRow;
    }
  }

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    StructTypeInfo rootType = new StructTypeInfo();
    // The source column names for ORC serde that will be used in the schema.
    rootType.setAllStructFieldNames(getColumnNames());
    rootType.setAllStructFieldTypeInfos(getColumnTypes());
    inspector = OrcStruct.createObjectInspector(rootType);
  }

  /**
   * NOTE: if "columns.types" is missing, all columns will be of String type.
   */
  @Override
  protected List<TypeInfo> parseColumnTypes() {
    final List<TypeInfo> fieldTypes = super.parseColumnTypes();
    if (fieldTypes.isEmpty()) {
      return Collections.nCopies(getColumnNames().size(), TypeInfoFactory.stringTypeInfo);
    }
    return fieldTypes;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return OrcSerdeRow.class;
  }

  @Override
  public Writable serialize(Object realRow, ObjectInspector inspector) {
    row.realRow = realRow;
    row.inspector = inspector;
    return row;
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    return writable;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public List<FieldSchema> readSchema(Configuration conf, String file) throws SerDeException {
    List<String> fieldNames;
    List<TypeDescription> fieldTypes;
    try (Reader reader = OrcFile.createReader(new Path(file), OrcFile.readerOptions(conf))) {
      fieldNames = reader.getSchema().getFieldNames();
      fieldTypes = reader.getSchema().getChildren();
    } catch (Exception e) {
      throw new SerDeException(ErrorMsg.ORC_FOOTER_ERROR.getErrorCodedMsg(), e);
    }

    List<FieldSchema> schema = new ArrayList<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      FieldSchema fieldSchema = convertOrcTypeToFieldSchema(fieldNames.get(i), fieldTypes.get(i));
      schema.add(fieldSchema);
      LOG.debug("Inferred field schema {}", fieldSchema);
    }
    return schema;
  }

  private FieldSchema convertOrcTypeToFieldSchema(String fieldName, TypeDescription fieldType) {
    String typeName = convertOrcTypeToFieldType(fieldType);
    return new FieldSchema(fieldName, typeName, "Inferred from Orc file.");
  }

  private String convertOrcTypeToFieldType(TypeDescription fieldType) {
    if (fieldType.getCategory().isPrimitive()) {
      return convertPrimitiveType(fieldType);
    }
    return convertComplexType(fieldType);
  }

  private String convertPrimitiveType(TypeDescription fieldType) {
    if (fieldType.getCategory().getName().equals("timestamp with local time zone")) {
      throw new IllegalArgumentException("Unhandled ORC type " + fieldType.getCategory().getName());
    }
    return fieldType.toString();
  }

  private String convertComplexType(TypeDescription fieldType) {
    StringBuilder buffer = new StringBuilder();
    buffer.append(fieldType.getCategory().getName());
    switch (fieldType.getCategory()) {
    case LIST:
    case MAP:
    case UNION:
      buffer.append('<');
      for (int i = 0; i < fieldType.getChildren().size(); i++) {
        if (i != 0) {
          buffer.append(',');
        }
        buffer.append(convertOrcTypeToFieldType(fieldType.getChildren().get(i)));
      }
      buffer.append('>');
      break;
    case STRUCT:
      buffer.append('<');
      for (int i = 0; i < fieldType.getChildren().size(); ++i) {
        if (i != 0) {
          buffer.append(',');
        }
        getStructFieldName(buffer, fieldType.getFieldNames().get(i));
        buffer.append(':');
        buffer.append(convertOrcTypeToFieldType(fieldType.getChildren().get(i)));
      }
      buffer.append('>');
      break;
    default:
      throw new IllegalArgumentException("ORC doesn't handle " +
          fieldType.getCategory());
    }
    return buffer.toString();
  }

  static void getStructFieldName(StringBuilder buffer, String name) {
    if (UNQUOTED_NAMES.matcher(name).matches()) {
      buffer.append(name);
    } else {
      buffer.append('`').append(name.replace("`", "``")).append('`');
    }
  }
}
