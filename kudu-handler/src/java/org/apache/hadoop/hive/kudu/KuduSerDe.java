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
package org.apache.hadoop.hive.kudu;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.PartialRow;

import static org.apache.hadoop.hive.kudu.KuduHiveUtils.createOverlayedConf;
import static org.apache.hadoop.hive.kudu.KuduHiveUtils.toHiveType;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_TABLE_NAME_KEY;

/**
 * A Kudu serializer and deserializer to support reading and writing Kudu data from Hive.
 */
@SerDeSpec(schemaProps = { KuduStorageHandler.KUDU_TABLE_NAME_KEY })
public class KuduSerDe extends AbstractSerDe {

  private ObjectInspector objectInspector;
  private Schema schema;


  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    Configuration conf = createOverlayedConf(configuration, properties);
    String tableName = conf.get(KuduStorageHandler.KUDU_TABLE_NAME_KEY);
    if (StringUtils.isEmpty(tableName)) {
      throw new SerDeException(KUDU_TABLE_NAME_KEY + " is not set.");
    }
    try (KuduClient client = KuduHiveUtils.getKuduClient(conf)) {
      if (!client.tableExists(tableName)) {
        throw new SerDeException("Kudu table does not exist: " + tableName);
      }
      schema = client.openTable(tableName).getSchema();
    } catch (IOException ex) {
      throw new SerDeException(ex);
    }
    this.objectInspector = createObjectInspector(schema);
  }

  private static ObjectInspector createObjectInspector(Schema schema) throws SerDeException {
    Preconditions.checkNotNull(schema);
    List<String> fieldNames = new ArrayList<>();
    List<ObjectInspector> fieldInspectors = new ArrayList<>();
    List<String> fieldComments = new ArrayList<>();
    for (int i = 0; i < schema.getColumnCount(); i++) {
      ColumnSchema col = schema.getColumnByIndex(i);
      PrimitiveTypeInfo typeInfo = toHiveType(col.getType(), col.getTypeAttributes());
      fieldNames.add(col.getName());
      fieldInspectors.add(
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo));
      fieldComments.add(col.getComment());
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldInspectors,
        fieldComments);
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return objectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return KuduWritable.class;
  }

  /**
   * Serialize an object by navigating inside the Object with the ObjectInspector.
   */
  @Override
  public KuduWritable serialize(Object obj, ObjectInspector objectInspector) throws SerDeException {
    Preconditions.checkArgument(objectInspector.getCategory() == Category.STRUCT);

    StructObjectInspector soi = (StructObjectInspector) objectInspector;
    List<Object> writableObj = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    PartialRow row = schema.newPartialRow();
    for (int i = 0; i < schema.getColumnCount(); i++) {
      StructField field = fields.get(i);
      Object value = writableObj.get(i);

      if (value == null) {
        row.setNull(i);
      } else {
        Type type = schema.getColumnByIndex(i).getType();
        ObjectInspector inspector = field.getFieldObjectInspector();
        switch (type) {
        case BOOL:
          boolean boolVal = ((BooleanObjectInspector) inspector).get(value);
          row.addBoolean(i, boolVal);
          break;
        case INT8:
          byte byteVal = ((ByteObjectInspector) inspector).get(value);
          row.addByte(i, byteVal);
          break;
        case INT16:
          short shortVal = ((ShortObjectInspector) inspector).get(value);
          row.addShort(i, shortVal);
          break;
        case INT32:
          int intVal = ((IntObjectInspector) inspector).get(value);
          row.addInt(i, intVal);
          break;
        case INT64:
          long longVal = ((LongObjectInspector) inspector).get(value);
          row.addLong(i, longVal);
          break;
        case UNIXTIME_MICROS:
          // Calling toSqlTimestamp and using the addTimestamp API ensures we properly
          // convert Hive localDateTime to UTC.
          java.sql.Timestamp timestampVal = ((TimestampObjectInspector) inspector)
              .getPrimitiveJavaObject(value).toSqlTimestamp();
          row.addTimestamp(i, timestampVal);
          break;
        case DECIMAL:
          HiveDecimal decimalVal = ((HiveDecimalObjectInspector) inspector)
              .getPrimitiveJavaObject(value);
          row.addDecimal(i, decimalVal.bigDecimalValue());
          break;
        case FLOAT:
          float floatVal = ((FloatObjectInspector) inspector).get(value);
          row.addFloat(i, floatVal);
          break;
        case DOUBLE:
          double doubleVal = ((DoubleObjectInspector) inspector).get(value);
          row.addDouble(i, doubleVal);
          break;
        case STRING:
          String stringVal = ((StringObjectInspector) inspector).getPrimitiveJavaObject(value);
          row.addString(i, stringVal);
          break;
        case BINARY:
          byte[] bytesVal = ((BinaryObjectInspector) inspector).getPrimitiveJavaObject(value);
          row.addBinary(i, bytesVal);
          break;
        default:
          throw new SerDeException("Unsupported column type: " + type.name());
        }
      }
    }
    return new KuduWritable(row);
  }

  /**
   * Deserialize an object out of a Writable blob.
   */
  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    KuduWritable input = (KuduWritable) writable;
    List<Object> output = new ArrayList<>();
    for(int i  = 0; i < schema.getColumnCount(); i++) {
      // If the column isn't set, skip it.
      if (!input.isSet(i)) {
        continue;
      }
      Object javaObj = input.getValueObject(i);
      ColumnSchema col = schema.getColumnByIndex(i);
      PrimitiveTypeInfo typeInfo = toHiveType(col.getType(), col.getTypeAttributes());
      if (javaObj == null) {
        output.add(null);
      } else {
        switch (typeInfo.getPrimitiveCategory()) {
        case BOOLEAN:
          output.add(new BooleanWritable((boolean) javaObj));
          break;
        case BYTE:
          output.add(new ByteWritable((byte) javaObj));
          break;
        case SHORT:
          output.add(new ShortWritable((short) javaObj));
          break;
        case INT:
          output.add(new IntWritable((int) javaObj));
          break;
        case LONG:
          output.add(new LongWritable((long) javaObj));
          break;
        case TIMESTAMP:
          java.sql.Timestamp sqlTs = (java.sql.Timestamp) javaObj;
          Timestamp hiveTs = Timestamp.ofEpochMilli(sqlTs.getTime(), sqlTs.getNanos());
          output.add(new TimestampWritableV2(hiveTs));
          break;
        case DECIMAL:
          HiveDecimal hiveDecimal = HiveDecimal.create((BigDecimal) javaObj);
          output.add(new HiveDecimalWritable(hiveDecimal));
          break;
        case FLOAT:
          output.add(new FloatWritable((float) javaObj));
          break;
        case DOUBLE:
          output.add(new DoubleWritable((double) javaObj));
          break;
        case STRING:
          output.add(new Text((String) javaObj));
          break;
        case BINARY:
          output.add(new BytesWritable((byte[]) javaObj));
          break;
        default:
          throw new SerDeException("Unsupported type: " + typeInfo.getPrimitiveCategory());
        }
      }
    }
    return output;
  }
}


