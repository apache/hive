/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.ParquetWriter;

/**
 *
 * A ParquetHiveSerDe for Hive (with the deprecated package mapred)
 *
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
        ParquetOutputFormat.COMPRESSION})
public class ParquetHiveSerDe extends AbstractSerDe {
  public static final Text MAP_KEY = new Text("key");
  public static final Text MAP_VALUE = new Text("value");
  public static final Text MAP = new Text("map");
  public static final Text ARRAY = new Text("bag");

  // default compression type for parquet output format
  private static final String DEFAULTCOMPRESSION =
          ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME.name();

  // Map precision to the number bytes needed for binary conversion.
  public static final int PRECISION_TO_BYTE_COUNT[] = new int[38];
  static {
    for (int prec = 1; prec <= 38; prec++) {
      // Estimated number of bytes needed.
      PRECISION_TO_BYTE_COUNT[prec - 1] = (int)
          Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
    }
  }

  private SerDeStats stats;
  private ObjectInspector objInspector;

  private enum LAST_OPERATION {
    SERIALIZE,
    DESERIALIZE,
    UNKNOWN
  }

  private LAST_OPERATION status;
  private long serializedSize;
  private long deserializedSize;
  private String compressionType;

  private ParquetHiveRecord parquetRow;

  public ParquetHiveSerDe() {
    parquetRow = new ParquetHiveRecord();
    stats = new SerDeStats();
  }

  @Override
  public final void initialize(final Configuration conf, final Properties tbl) throws SerDeException {

    final TypeInfo rowTypeInfo;
    final List<String> columnNames;
    final List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    // Get compression properties
    compressionType = tbl.getProperty(ParquetOutputFormat.COMPRESSION, DEFAULTCOMPRESSION);

    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    if (columnNames.size() != columnTypes.size()) {
      throw new IllegalArgumentException("ParquetHiveSerde initialization failed. Number of column " +
        "name and column type differs. columnNames = " + columnNames + ", columnTypes = " +
        columnTypes);
    }
    // Create row related objects
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    this.objInspector = new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);

    // Stats part
    serializedSize = 0;
    deserializedSize = 0;
    status = LAST_OPERATION.UNKNOWN;
  }

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    status = LAST_OPERATION.DESERIALIZE;
    deserializedSize = 0;
    if (blob instanceof ArrayWritable) {
      deserializedSize = ((ArrayWritable) blob).get().length;
      return blob;
    } else {
      return null;
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return ParquetHiveRecord.class;
  }

  @Override
  public Writable serialize(final Object obj, final ObjectInspector objInspector)
      throws SerDeException {
    if (!objInspector.getCategory().equals(Category.STRUCT)) {
      throw new SerDeException("Cannot serialize " + objInspector.getCategory() + ". Can only serialize a struct");
    }
    serializedSize = ((StructObjectInspector)objInspector).getAllStructFieldRefs().size();
    status = LAST_OPERATION.SERIALIZE;
    parquetRow.value = obj;
    parquetRow.inspector= (StructObjectInspector)objInspector;
    return parquetRow;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // must be different
    assert (status != LAST_OPERATION.UNKNOWN);
    if (status == LAST_OPERATION.SERIALIZE) {
      stats.setRawDataSize(serializedSize);
    } else {
      stats.setRawDataSize(deserializedSize);
    }
    return stats;
  }
}
