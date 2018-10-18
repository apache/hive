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

package org.apache.hadoop.hive.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Generic Kafka Serde that allow user to delegate Serde to other class like Avro,
 * Json or any class that supports {@link BytesWritable}.
 * I the user which to implement their own serde all they need is to implement a serde that extend
 * {@link org.apache.hadoop.hive.serde2.AbstractSerDe} and accept {@link BytesWritable} as value
 */
@SerDeSpec(schemaProps = { serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES }) public class KafkaSerDe
    extends AbstractSerDe {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSerDe.class);

  /**
   * Delegate SerDe used to Serialize and DeSerialize data form/to Kafka.
   */
  private AbstractSerDe delegateSerDe;

  /**
   * Delegate Object Inspector used to Deserialize the row, this OI is constructed by the {@code delegateSerDe}.
   */
  private StructObjectInspector delegateDeserializerOI;

  /**
   * Delegate Object Inspector used to Serialize the row as byte array.
   */
  private StructObjectInspector delegateSerializerOI;

  /**
   * Object Inspector of original row plus metadata.
   */
  private ObjectInspector objectInspector;
  private final List<String> columnNames = Lists.newArrayList();
  private BytesConverter bytesConverter;

  @Override public void initialize(@Nullable Configuration conf, Properties tbl) throws SerDeException {
    //This method is called before {@link org.apache.hadoop.hive.kafka.KafkaStorageHandler.preCreateTable}
    //Thus we need to default to org.apache.hadoop.hive.kafka.KafkaUtils.DEFAULT_PROPERTIES if any property is needed
    final String
        className =
        tbl.getProperty(KafkaTableProperties.SERDE_CLASS_NAME.getName(),
            KafkaTableProperties.SERDE_CLASS_NAME.getDefaultValue());
    delegateSerDe = KafkaUtils.createDelegate(className);
    //noinspection deprecation
    delegateSerDe.initialize(conf, tbl);

    if (!(delegateSerDe.getObjectInspector() instanceof StructObjectInspector)) {
      throw new SerDeException("Was expecting Struct Object Inspector but have " + delegateSerDe.getObjectInspector()
          .getClass()
          .getName());
    }
    delegateDeserializerOI = (StructObjectInspector) delegateSerDe.getObjectInspector();

    // Build column names Order matters here
    columnNames.addAll(delegateDeserializerOI.getAllStructFieldRefs()
        .stream()
        .map(StructField::getFieldName)
        .collect(Collectors.toList()));
    columnNames.addAll(MetadataColumn.KAFKA_METADATA_COLUMN_NAMES);

    final List<ObjectInspector> inspectors = new ArrayList<>(columnNames.size());
    inspectors.addAll(delegateDeserializerOI.getAllStructFieldRefs()
        .stream()
        .map(StructField::getFieldObjectInspector)
        .collect(Collectors.toList()));
    inspectors.addAll(MetadataColumn.KAFKA_METADATA_INSPECTORS);
    objectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);

    // Setup Read and Write Path From/To Kafka
    if (delegateSerDe.getSerializedClass() == Text.class) {
      bytesConverter = new TextBytesConverter();
    } else if (delegateSerDe.getSerializedClass() == AvroGenericRecordWritable.class) {
      String schemaFromProperty = tbl.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), "");
      Preconditions.checkArgument(!schemaFromProperty.isEmpty(), "Avro Schema is empty Can not go further");
      Schema schema = AvroSerdeUtils.getSchemaFor(schemaFromProperty);
      LOG.debug("Building Avro Reader with schema {}", schemaFromProperty);
      bytesConverter = new AvroBytesConverter(schema);
    } else {
      bytesConverter = new BytesWritableConverter();
    }
  }

  @Override public Class<? extends Writable> getSerializedClass() {
    return delegateSerDe.getSerializedClass();
  }

  @Override public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {

    if (!(objInspector instanceof StructObjectInspector)) {
      throw new SerDeException("Object inspector has to be "
          + StructObjectInspector.class.getName()
          + " but got "
          + objInspector.getClass().getName());
    }
    StructObjectInspector structObjectInspector = (StructObjectInspector) objInspector;
    List<Object> data = structObjectInspector.getStructFieldsDataAsList(obj);
    if (delegateSerializerOI == null) {
      //@TODO check if i can cache this if it is the same.
      delegateSerializerOI =
          new SubStructObjectInspector(structObjectInspector, data.size() - MetadataColumn.values().length);
    }
    // We always append the metadata columns to the end of the row.
    final List<Object> row = data.subList(0, data.size() - MetadataColumn.values().length);
    //@TODO @FIXME use column names instead of actual positions that can be hard to read and review
    Object key = data.get(data.size() - MetadataColumn.KAFKA_METADATA_COLUMN_NAMES.size());
    Object partition = data.get(data.size() - MetadataColumn.KAFKA_METADATA_COLUMN_NAMES.size() + 1);
    Object offset = data.get(data.size() - MetadataColumn.KAFKA_METADATA_COLUMN_NAMES.size() + 2);
    Object timestamp = data.get(data.size() - MetadataColumn.KAFKA_METADATA_COLUMN_NAMES.size() + 3);

    if (PrimitiveObjectInspectorUtils.getLong(offset, MetadataColumn.OFFSET.getObjectInspector()) != -1) {
      LOG.error("Can not insert values into `__offset` column, has to be [-1]");
      throw new SerDeException("Can not insert values into `__offset` column, has to be [-1]");
    }

    final byte[]
        keyBytes =
        key == null ? null : PrimitiveObjectInspectorFactory.writableBinaryObjectInspector.getPrimitiveJavaObject(key);
    final long
        recordTs =
        timestamp == null ?
            -1 :
            PrimitiveObjectInspectorUtils.getLong(timestamp, MetadataColumn.TIMESTAMP.getObjectInspector());
    final int
        recordPartition =
        partition == null ?
            -1 :
            PrimitiveObjectInspectorUtils.getInt(partition, MetadataColumn.PARTITION.getObjectInspector());

    //noinspection unchecked
    return new KafkaWritable(recordPartition,
        recordTs,
        bytesConverter.getBytes(delegateSerDe.serialize(row, delegateSerializerOI)),
        keyBytes);
  }

  @Override public SerDeStats getSerDeStats() {
    return delegateSerDe.getSerDeStats();
  }

  @Override public Object deserialize(Writable blob) throws SerDeException {
    KafkaWritable record = (KafkaWritable) blob;
    final Object row = delegateSerDe.deserialize(bytesConverter.getWritable(record.getValue()));
    return columnNames.stream().map(name -> {
      final MetadataColumn metadataColumn = MetadataColumn.forName(name);
      if (metadataColumn != null) {
        return record.getHiveWritable(metadataColumn);
      }
      return delegateDeserializerOI.getStructFieldData(row, delegateDeserializerOI.getStructFieldRef(name));
    }).collect(Collectors.toList());
  }

  @Override public ObjectInspector getObjectInspector() {
    return objectInspector;
  }

  /**
   * Returns a view of input object inspector list between:
   * <tt>0</tt> inclusive and the specified <tt>toIndex</tt>, exclusive.
   */
  private static final class SubStructObjectInspector extends StructObjectInspector {

    private final StructObjectInspector baseOI;
    private final List<? extends StructField> structFields;

    /**
     * Returns a live view of the base Object inspector starting form 0 to {@code toIndex} exclusive.
     * @param baseOI base Object Inspector.
     * @param toIndex toIndex.
     */
    private SubStructObjectInspector(StructObjectInspector baseOI, int toIndex) {
      this.baseOI = baseOI;
      structFields = baseOI.getAllStructFieldRefs().subList(0, toIndex);
    }

    /**
     * Returns all the fields.
     */
    @Override public List<? extends StructField> getAllStructFieldRefs() {
      return structFields;
    }

    /**
     * Look up a field.
     * @param fieldName fieldName to be looked up.
     */
    @SuppressWarnings("OptionalGetWithoutIsPresent") @Override public StructField getStructFieldRef(String fieldName) {
      return this.getAllStructFieldRefs()
          .stream()
          .filter(ref -> ref.getFieldName().equals(fieldName))
          .findFirst()
          .get();
    }

    /**
     * returns null for data = null.
     * @param data input.
     * @param fieldRef field to extract.
     */
    @Override public Object getStructFieldData(Object data, StructField fieldRef) {
      return baseOI.getStructFieldData(data, fieldRef);
    }

    /**
     * returns null for data = null.
     * @param data input data.
     */
    @Override public List<Object> getStructFieldsDataAsList(Object data) {
      if (data == null) {
        return null;
      }
      int size = getAllStructFieldRefs().size();
      List<Object> res = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        res.add(baseOI.getStructFieldData(data, getAllStructFieldRefs().get(i)));
      }
      return res;
    }

    /**
     * Returns the name of the data type that is inspected by this
     * ObjectInspector. This is used to display the type information to the user.
     *
     * For primitive types, the type name is standardized. For other types, the
     * type name can be something like "list&lt;int&gt;", "map&lt;int,string&gt;", java class
     * names, or user-defined type names similar to typedef.
     */
    @Override public String getTypeName() {
      return baseOI.getTypeName();
    }

    /**
     * An ObjectInspector must inherit from one of the following interfaces if
     * getCategory() returns: PRIMITIVE: PrimitiveObjectInspector LIST:
     * ListObjectInspector MAP: MapObjectInspector STRUCT: StructObjectInspector.
     */
    @Override public Category getCategory() {
      return baseOI.getCategory();
    }
  }

  /**
   * Class that encapsulate the logic of serialize and deserialize bytes array to/from the delegate writable format.
   * @param <K> delegate writable class.
   */
  private interface BytesConverter<K extends Writable> {
    byte[] getBytes(K writable);

    K getWritable(byte[] value);
  }

  private static class AvroBytesConverter implements BytesConverter<AvroGenericRecordWritable> {
    private final Schema schema;
    private final DatumReader<GenericRecord> dataReader;
    private final GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<>();
    private final AvroGenericRecordWritable avroGenericRecordWritable = new AvroGenericRecordWritable();
    private final UID uid = new UID();

    AvroBytesConverter(Schema schema) {
      this.schema = schema;
      dataReader = new SpecificDatumReader<>(this.schema);
    }

    @Override public byte[] getBytes(AvroGenericRecordWritable writable) {
      GenericRecord record = writable.getRecord();
      byte[] valueBytes = null;
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        BinaryEncoder be = EncoderFactory.get().directBinaryEncoder(out, null);
        gdw.setSchema(record.getSchema());
        gdw.write(record, be);
        out.flush();
        valueBytes = out.toByteArray();
      } catch (IOException e) {
        Throwables.propagate(new SerDeException(e));
      }
      return valueBytes;
    }

    @Override public AvroGenericRecordWritable getWritable(byte[] value) {
      GenericRecord avroRecord = null;
      try {
        avroRecord = dataReader.read(null, DecoderFactory.get().binaryDecoder(value, null));
      } catch (IOException e) {
        Throwables.propagate(new SerDeException(e));
      }

      avroGenericRecordWritable.setRecord(avroRecord);
      avroGenericRecordWritable.setRecordReaderID(uid);
      avroGenericRecordWritable.setFileSchema(avroRecord.getSchema());
      return avroGenericRecordWritable;
    }
  }

  private static class BytesWritableConverter implements BytesConverter<BytesWritable> {
    @Override public byte[] getBytes(BytesWritable writable) {
      return writable.getBytes();
    }

    @Override public BytesWritable getWritable(byte[] value) {
      return new BytesWritable(value);
    }
  }

  private static class TextBytesConverter implements BytesConverter<Text> {
    final private Text text = new Text();
    @Override public byte[] getBytes(Text writable) {
      //@TODO  There is no reason to decode then encode the string to bytes really
      //@FIXME this issue with CTRL-CHAR ^0 added by Text at the end of string and Json serd does not like that.
      try {
        return Text.decode(writable.getBytes(), 0, writable.getLength()).getBytes(Charset.forName("UTF-8"));
      } catch (CharacterCodingException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public Text getWritable(byte[] value) {
      text.set(value);
      return text;
    }
  }

}
