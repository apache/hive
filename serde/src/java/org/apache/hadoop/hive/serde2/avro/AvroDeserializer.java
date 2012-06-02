/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

class AvroDeserializer {
  private static final Log LOG = LogFactory.getLog(AvroDeserializer.class);
  /**
   * When encountering a record with an older schema than the one we're trying
   * to read, it is necessary to re-encode with a reader against the newer schema.
   * Because Hive doesn't provide a way to pass extra information to the
   * inputformat, we're unable to provide the newer schema when we have it and it
   * would be most useful - when the inputformat is reading the file.
   *
   * This is a slow process, so we try to cache as many of the objects as possible.
   */
  static class SchemaReEncoder {
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<GenericRecord>();
    private BinaryDecoder binaryDecoder = null;
    private InstanceCache<ReaderWriterSchemaPair, GenericDatumReader<GenericRecord>> gdrCache
        = new InstanceCache<ReaderWriterSchemaPair, GenericDatumReader<GenericRecord>>() {
            @Override
            protected GenericDatumReader<GenericRecord> makeInstance(ReaderWriterSchemaPair hv) {
              return new GenericDatumReader<GenericRecord>(hv.getWriter(), hv.getReader());
            }
          };

    public GenericRecord reencode(GenericRecord r, Schema readerSchema)
            throws AvroSerdeException {
      baos.reset();

      BinaryEncoder be = EncoderFactory.get().directBinaryEncoder(baos, null);
      gdw.setSchema(r.getSchema());
      try {
        gdw.write(r, be);
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

        binaryDecoder = DecoderFactory.defaultFactory().createBinaryDecoder(bais, binaryDecoder);

        ReaderWriterSchemaPair pair = new ReaderWriterSchemaPair(r.getSchema(), readerSchema);
        GenericDatumReader<GenericRecord> gdr = gdrCache.retrieve(pair);
        return gdr.read(r, binaryDecoder);

      } catch (IOException e) {
        throw new AvroSerdeException("Exception trying to re-encode record to new schema", e);
      }
    }
  }

  private List<Object> row;
  private SchemaReEncoder reEncoder;

  /**
   * Deserialize an Avro record, recursing into its component fields and
   * deserializing them as well.  Fields of the record are matched by name
   * against fields in the Hive row.
   *
   * Because Avro has some data types that Hive does not, these are converted
   * during deserialization to types Hive will work with.
   *
   * @param columnNames List of columns Hive is expecting from record.
   * @param columnTypes List of column types matched by index to names
   * @param writable Instance of GenericAvroWritable to deserialize
   * @param readerSchema Schema of the writable to deserialize
   * @return A list of objects suitable for Hive to work with further
   * @throws AvroSerdeException For any exception during deseriliazation
   */
  public Object deserialize(List<String> columnNames, List<TypeInfo> columnTypes,
                            Writable writable, Schema readerSchema) throws AvroSerdeException {
    if(!(writable instanceof AvroGenericRecordWritable))
      throw new AvroSerdeException("Expecting a AvroGenericRecordWritable");

    if(row == null || row.size() != columnNames.size())
      row = new ArrayList<Object>(columnNames.size());
    else
      row.clear();

    AvroGenericRecordWritable recordWritable = (AvroGenericRecordWritable) writable;
    GenericRecord r = recordWritable.getRecord();

    // Check if we're working with an evolved schema
    if(!r.getSchema().equals(readerSchema)) {
      LOG.warn("Received different schemas.  Have to re-encode: " +
              r.getSchema().toString(false));
      if(reEncoder == null) reEncoder = new SchemaReEncoder();
      r = reEncoder.reencode(r, readerSchema);
    }

    workerBase(row, columnNames, columnTypes, r);
    return row;
  }

  // The actual deserialization may involve nested records, which require recursion.
  private List<Object> workerBase(List<Object> objectRow, List<String> columnNames,
                                  List<TypeInfo> columnTypes, GenericRecord record)
          throws AvroSerdeException {
    for(int i = 0; i < columnNames.size(); i++) {
      TypeInfo columnType = columnTypes.get(i);
      String columnName = columnNames.get(i);
      Object datum = record.get(columnName);
      Schema datumSchema = record.getSchema().getField(columnName).schema();

      objectRow.add(worker(datum, datumSchema, columnType));
    }

    return objectRow;
  }

  private Object worker(Object datum, Schema recordSchema, TypeInfo columnType)
          throws AvroSerdeException {
    // Klaxon! Klaxon! Klaxon!
    // Avro requires NULLable types to be defined as unions of some type T
    // and NULL.  This is annoying and we're going to hide it from the user.
    if(AvroSerdeUtils.isNullableType(recordSchema))
      return deserializeNullableUnion(datum, recordSchema, columnType);

    if(columnType == TypeInfoFactory.stringTypeInfo)
      return datum.toString(); // To workaround AvroUTF8
      // This also gets us around the Enum issue since we just take the value
      // and convert it to a string. Yay!

    switch(columnType.getCategory()) {
    case STRUCT:
      return deserializeStruct((GenericData.Record) datum, (StructTypeInfo) columnType);
     case UNION:
      return deserializeUnion(datum, recordSchema, (UnionTypeInfo) columnType);
    case LIST:
      return deserializeList(datum, recordSchema, (ListTypeInfo) columnType);
    case MAP:
      return deserializeMap(datum, recordSchema, (MapTypeInfo) columnType);
    default:
      return datum; // Simple type.
    }
  }

  /**
   * Extract either a null or the correct type from a Nullable type.  This is
   * horrible in that we rebuild the TypeInfo every time.
   */
  private Object deserializeNullableUnion(Object datum, Schema recordSchema,
                                          TypeInfo columnType) throws AvroSerdeException {
    int tag = GenericData.get().resolveUnion(recordSchema, datum); // Determine index of value
    Schema schema = recordSchema.getTypes().get(tag);
    if(schema.getType().equals(Schema.Type.NULL))
      return null;
    return worker(datum, schema, SchemaToTypeInfo.generateTypeInfo(schema));

  }

  private Object deserializeStruct(GenericData.Record datum, StructTypeInfo columnType)
          throws AvroSerdeException {
    // No equivalent Java type for the backing structure, need to recurse and build a list
    ArrayList<TypeInfo> innerFieldTypes = columnType.getAllStructFieldTypeInfos();
    ArrayList<String> innerFieldNames = columnType.getAllStructFieldNames();
    List<Object> innerObjectRow = new ArrayList<Object>(innerFieldTypes.size());

    return workerBase(innerObjectRow, innerFieldNames, innerFieldTypes, datum);
  }

  private Object deserializeUnion(Object datum, Schema recordSchema,
                                  UnionTypeInfo columnType) throws AvroSerdeException {
    int tag = GenericData.get().resolveUnion(recordSchema, datum); // Determine index of value
    Object desered = worker(datum, recordSchema.getTypes().get(tag),
            columnType.getAllUnionObjectTypeInfos().get(tag));
    return new StandardUnionObjectInspector.StandardUnion((byte)tag, desered);
  }

  private Object deserializeList(Object datum, Schema recordSchema,
                                 ListTypeInfo columnType) throws AvroSerdeException {
    // Need to check the original schema to see if this is actually a Fixed.
    if(recordSchema.getType().equals(Schema.Type.FIXED)) {
    // We're faking out Hive to work through a type system impedence mismatch.
    // Pull out the backing array and convert to a list.
      GenericData.Fixed fixed = (GenericData.Fixed) datum;
      List<Byte> asList = new ArrayList<Byte>(fixed.bytes().length);
      for(int j = 0; j < fixed.bytes().length; j++) {
        asList.add(fixed.bytes()[j]);
      }
      return asList;
    } else if(recordSchema.getType().equals(Schema.Type.BYTES)) {
      // This is going to be slow... hold on.
      ByteBuffer bb = (ByteBuffer)datum;
      List<Byte> asList = new ArrayList<Byte>(bb.capacity());
      byte[] array = bb.array();
      for(int j = 0; j < array.length; j++) {
        asList.add(array[j]);
      }
      return asList;
    } else { // An actual list, deser its values
      List listData = (List) datum;
      Schema listSchema = recordSchema.getElementType();
      List<Object> listContents = new ArrayList<Object>(listData.size());
      for(Object obj : listData) {
        listContents.add(worker(obj, listSchema, columnType.getListElementTypeInfo()));
      }
      return listContents;
    }
  }

  private Object deserializeMap(Object datum, Schema mapSchema, MapTypeInfo columnType)
          throws AvroSerdeException {
    // Avro only allows maps with Strings for keys, so we only have to worry
    // about deserializing the values
    Map<String, Object> map = new Hashtable<String, Object>();
    Map<Utf8, Object> mapDatum = (Map)datum;
    Schema valueSchema = mapSchema.getValueType();
    TypeInfo valueTypeInfo = columnType.getMapValueTypeInfo();
    for (Utf8 key : mapDatum.keySet()) {
      Object value = mapDatum.get(key);
      map.put(key.toString(), worker(value, valueSchema, valueTypeInfo));
    }

    return map;
  }
}
