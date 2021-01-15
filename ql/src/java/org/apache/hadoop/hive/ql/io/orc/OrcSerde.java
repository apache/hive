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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Writable;

/**
 * A serde class for ORC. It transparently passes the object to/from the ORC
 * file reader/writer. This SerDe does not support statistics, since serialized
 * size doesn't make sense in the context of ORC files.
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES, OrcSerde.COMPRESSION})
public class OrcSerde extends AbstractSerDe {

  private final OrcSerdeRow row = new OrcSerdeRow();
  private ObjectInspector inspector = null;

  static final String COMPRESSION = "orc.compress";

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

}
