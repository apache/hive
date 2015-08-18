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
package org.apache.hadoop.hive.ql.io.orc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSerde;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

/**
 * A serde class for ORC.
 * It transparently passes the object to/from the ORC file reader/writer.
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES})
public class OrcSerde implements SerDe, VectorizedSerde {

  private static final Log LOG = LogFactory.getLog(OrcSerde.class);

  private final OrcSerdeRow row = new OrcSerdeRow();
  private ObjectInspector inspector = null;

  private VectorizedOrcSerde vos = null;

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
  public void initialize(Configuration conf, Properties table) {
    // Read the configuration parameters
    String columnNameProperty = table.getProperty(serdeConstants.LIST_COLUMNS);
    // NOTE: if "columns.types" is missing, all columns will be of String type
    String columnTypeProperty = table.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    // Parse the configuration parameters
    ArrayList<String> columnNames = new ArrayList<String>();
    if (columnNameProperty != null && columnNameProperty.length() > 0) {
      for (String name : columnNameProperty.split(",")) {
        columnNames.add(name);
      }
    }
    if (columnTypeProperty == null) {
      // Default type: all string
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columnNames.size(); i++) {
        if (i > 0) {
          sb.append(":");
        }
        sb.append("string");
      }
      columnTypeProperty = sb.toString();
    }

    ArrayList<TypeInfo> fieldTypes =
        TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    StructTypeInfo rootType = new StructTypeInfo();
    rootType.setAllStructFieldNames(columnNames);
    rootType.setAllStructFieldTypeInfos(fieldTypes);
    inspector = OrcStruct.createObjectInspector(rootType);
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

  /**
   * Always returns null, since serialized size doesn't make sense in the
   * context of ORC files.
   *
   * @return null
   */
  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Writable serializeVector(VectorizedRowBatch vrg, ObjectInspector objInspector)
      throws SerDeException {
    if (vos == null) {
      vos = new VectorizedOrcSerde(getObjectInspector());
    }
    return vos.serialize(vrg, getObjectInspector());
  }

  @Override
  public void deserializeVector(Object rowBlob, int rowsInBatch, VectorizedRowBatch reuseBatch)
      throws SerDeException {
    // nothing to do here
  }
}
