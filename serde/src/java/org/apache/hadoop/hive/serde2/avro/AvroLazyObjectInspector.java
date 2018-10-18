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
package org.apache.hadoop.hive.serde2.avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.lang.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyUnion;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyListObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyUnionObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;

/**
 * Lazy objectinspector for avro serialization
 * */
public class AvroLazyObjectInspector extends LazySimpleStructObjectInspector {

  /**
   * Reader {@link Schema} for the avro data
   * */
  private Schema readerSchema;

  /**
   * {@link AvroSchemaRetriever} to retrieve avro schema
   * */
  private AvroSchemaRetriever schemaRetriever;

  /**
   * LOGGER
   * */
  public static final Logger LOG = LoggerFactory.getLogger(AvroLazyObjectInspector.class);

  /**
   * Constructor
   *
   * @param structFieldNames fields within the given protobuf object
   * @param structFieldObjectInspectors object inspectors for the fields
   * @param structFieldComments comments for the given fields
   * @param separator separator between different fields
   * @param nullSequence sequence to represent null value
   * @param lastColumnTakesRest whether the last column of the struct should take the rest of the
   *          row if there are extra fields.
   * @param escaped whether the data is escaped or not
   * @param escapeChar if escaped is true, the escape character
   * */
  @Deprecated
  public AvroLazyObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, List<String> structFieldComments,
      byte separator, Text nullSequence, boolean lastColumnTakesRest, boolean escaped,
      byte escapeChar) {
    super(structFieldNames, structFieldObjectInspectors, structFieldComments, separator,
        nullSequence, lastColumnTakesRest, escaped, escapeChar);
  }

  public AvroLazyObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, List<String> structFieldComments,
      byte separator, LazyObjectInspectorParameters lazyParams) {
    super(structFieldNames, structFieldObjectInspectors, structFieldComments, separator, lazyParams);
  }

  /**
   * Set the reader schema for the {@link AvroLazyObjectInspector} to the given schema
   * */
  public void setReaderSchema(Schema readerSchema) {
    this.readerSchema = readerSchema;
  }

  /**
   * Set the {@link AvroSchemaRetriever} for the {@link AvroLazyObjectInspector} to the given class
   *
   * @param schemaRetriever the schema retriever class to be set
   * */
  public void setSchemaRetriever(AvroSchemaRetriever schemaRetriever) {
    this.schemaRetriever = schemaRetriever;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object getStructFieldData(Object data, StructField f) {
    if (data == null) {
      return null;
    }

    int fieldID = f.getFieldID();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting struct field data for field: [" + f.getFieldName() + "] on data ["
          + data.getClass() + "]");
    }

    if (data instanceof LazyStruct) {
      LazyStruct row = (LazyStruct) data;

      // get the field out of struct
      Object rowField = row.getField(fieldID);

      if (rowField instanceof LazyStruct) {

        if (LOG.isDebugEnabled() && rowField != null) {
          LOG.debug("Deserializing struct [" + rowField.getClass() + "]");
        }

        return deserializeStruct(rowField, f.getFieldName());

      } else if (rowField instanceof LazyMap) {
        // We have found a map. Systematically deserialize the values of the map and return back the
        // map
        LazyMap lazyMap = (LazyMap) rowField;

        for (Entry<Object, Object> entry : lazyMap.getMap().entrySet()) {
          Object _key = entry.getKey();
          Object _value = entry.getValue();

          if (_value instanceof LazyStruct) {
            lazyMap.getMap().put(_key, deserializeStruct(_value, f.getFieldName()));
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Returning a lazy map for field [" + f.getFieldName() + "]");
        }

        return lazyMap;

      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Returning [" + rowField + "] for field [" + f.getFieldName() + "]");
        }

        // Just return the object. We need no further operation on it
        return rowField;
      }
    } else {

      // The Avro deserializer would deserialize our object and return back a list of object that
      // hive can operate on. Here we should be getting the same object back.
      if (!(data instanceof List)) {
        throw new IllegalArgumentException("data should be an instance of list");
      }

      if (!(fieldID < ((List<Object>) data).size())) {
        return null;
      }

      // lookup the field corresponding to the given field ID and return
      Object field = ((List<Object>) data).get(fieldID);

      if (field == null) {
        return null;
      }

      // convert to a lazy object and return
      return toLazyObject(field, f.getFieldObjectInspector());
    }
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }

    List<Object> result = new ArrayList<Object>(fields.size());

    for (int i = 0; i < fields.size(); i++) {
      result.add(getStructFieldData(data, fields.get(i)));
    }

    return result;
  }

  /**
   * Deserialize the given struct object
   *
   * @param struct the object to deserialize
   * @param fieldName name of the field on which we are currently operating on
   * @return a deserialized object can hive can further operate on
   * @throws AvroObjectInspectorException if something goes wrong during deserialization
   * */
  private Object deserializeStruct(Object struct, String fieldName) {
    byte[] data = ((LazyStruct) struct).getBytes();
    AvroDeserializer deserializer = new AvroDeserializer();

    if (data == null || data.length == 0) {
      return null;
    }

    if (readerSchema == null && schemaRetriever == null) {
      throw new IllegalArgumentException("reader schema or schemaRetriever must be set for field ["
          + fieldName + "]");
    }

    Schema ws = null;
    Schema rs = null;
    int offset = 0;

    AvroGenericRecordWritable avroWritable = new AvroGenericRecordWritable();

    if (readerSchema == null) {
      offset = schemaRetriever.getOffset();

      if (data.length < offset) {
          throw new IllegalArgumentException("Data size cannot be less than [" + offset
              + "]. Found [" + data.length + "]");
      }

      rs = schemaRetriever.retrieveReaderSchema(data);

      if (rs == null) {
        // still nothing, Raise exception
        throw new IllegalStateException(
            "A valid reader schema could not be retrieved either directly or from the schema retriever for field ["
                + fieldName + "]");
      }

      ws = schemaRetriever.retrieveWriterSchema(data);

      if (ws == null) {
        throw new IllegalStateException(
            "Null writer schema retrieved from schemaRetriever for field [" + fieldName + "]");
      }

      // adjust the data bytes according to any possible offset that was provided
      if (LOG.isDebugEnabled()) {
        LOG.debug("Retrieved writer Schema: " + ws.toString());
        LOG.debug("Retrieved reader Schema: " + rs.toString());
      }

      try {
        avroWritable.readFields(data, offset, data.length, ws, rs);
      } catch (IOException ioe) {
        throw new AvroObjectInspectorException("Error deserializing avro payload", ioe);
      }
    } else {
      // a reader schema was provided
      if (schemaRetriever != null) {
        // a schema retriever has been provided as well. Attempt to read the write schema from the
        // retriever
        ws = schemaRetriever.retrieveWriterSchema(data);

        if (ws == null) {
          throw new IllegalStateException(
              "Null writer schema retrieved from schemaRetriever for field [" + fieldName + "]");
        }
      } else {
        // attempt retrieving the schema from the data
        ws = retrieveSchemaFromBytes(data);
      }

      rs = readerSchema;

      try {
        avroWritable.readFields(data, ws, rs);
      } catch (IOException ioe) {
        throw new AvroObjectInspectorException("Error deserializing avro payload", ioe);
      }
    }

    AvroObjectInspectorGenerator oiGenerator = null;
    Object deserializedObject = null;

    try {
      oiGenerator = new AvroObjectInspectorGenerator(rs);
      deserializedObject =
          deserializer.deserialize(oiGenerator.getColumnNames(), oiGenerator.getColumnTypes(),
              avroWritable, rs);
    } catch (SerDeException se) {
      throw new AvroObjectInspectorException("Error deserializing avro payload", se);
    }

    return deserializedObject;
  }

  /**
   * Retrieve schema from the given bytes
   *
   * @return the retrieved {@link Schema schema}
   * */
  private Schema retrieveSchemaFromBytes(byte[] data) {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();

    Schema schema = null;

    try {
      // dfs is AutoCloseable
      @SuppressWarnings("resource")
      DataFileStream<GenericRecord> dfs = new DataFileStream<GenericRecord>(bais, reader);
      schema = dfs.getSchema();
    } catch (IOException ioe) {
      throw new AvroObjectInspectorException("An error occurred retrieving schema from bytes", ioe);
    }

    return schema;
  }

  /**
   * Converts the given field to a lazy object
   *
   * @param field to be converted to a lazy object
   * @param fieldOI {@link ObjectInspector} for the given field
   * @return returns the converted lazy object
   * */
  private Object toLazyObject(Object field, ObjectInspector fieldOI) {
    if (isPrimitive(field.getClass())) {
      return toLazyPrimitiveObject(field, fieldOI);
    } else if (fieldOI instanceof LazyListObjectInspector) {
      return toLazyListObject(field, fieldOI);
    } else if (field instanceof StandardUnion) {
      return toLazyUnionObject(field, fieldOI);
    } else if (fieldOI instanceof LazyMapObjectInspector) {
      return toLazyMapObject(field, fieldOI);
    } else {
      return field;
    }
  }

  /**
   * Convert the given object to a lazy object using the given {@link ObjectInspector}
   *
   * @param obj Object to be converted to a {@link LazyObject}
   * @param oi ObjectInspector used for the conversion
   * @return the created {@link LazyObject lazy object}
   * */
  private LazyObject<? extends ObjectInspector> toLazyPrimitiveObject(Object obj, ObjectInspector oi) {
    if (obj == null) {
      return null;
    }

    LazyObject<? extends ObjectInspector> lazyObject = LazyFactory.createLazyObject(oi);
    ByteArrayRef ref = new ByteArrayRef();

    String objAsString = obj.toString().trim();

    ref.setData(objAsString.getBytes());

    // initialize the lazy object
    lazyObject.init(ref, 0, ref.getData().length);

    return lazyObject;
  }

  /**
   * Convert the given object to a lazy object using the given {@link ObjectInspector}
   *
   * @param obj Object to be converted to a {@link LazyObject}
   * @param objectInspector ObjectInspector used for the conversion
   * @return the created {@link LazyObject lazy object}
   * */
  private Object toLazyListObject(Object obj, ObjectInspector objectInspector) {
    if (obj == null) {
      return null;
    }

    List<?> listObj = (List<?>) obj;

    LazyArray retList = (LazyArray) LazyFactory.createLazyObject(objectInspector);

    List<Object> lazyList = retList.getList();

    ObjectInspector listElementOI =
        ((ListObjectInspector) objectInspector).getListElementObjectInspector();

    for (int i = 0; i < listObj.size(); i++) {
      lazyList.add(toLazyObject(listObj.get(i), listElementOI));
    }

    return retList;
  }

  /**
   * Convert the given object to a lazy object using the given {@link ObjectInspector}
   *
   * @param obj Object to be converted to a {@link LazyObject}
   * @param objectInspector ObjectInspector used for the conversion
   * @return the created {@link LazyObject lazy object}
   * */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Object toLazyMapObject(Object obj, ObjectInspector objectInspector) {
    if (obj == null) {
      return null;
    }

    // avro guarantees that the key will be of type string. So we just need to worry about
    // deserializing the value here

    LazyMap lazyMap = (LazyMap) LazyFactory.createLazyObject(objectInspector);

    Map map = lazyMap.getMap();

    Map<Object, Object> origMap = (Map) obj;

    ObjectInspector keyObjectInspector =
        ((MapObjectInspector) objectInspector).getMapKeyObjectInspector();
    ObjectInspector valueObjectInspector =
        ((MapObjectInspector) objectInspector).getMapValueObjectInspector();

    for (Entry entry : origMap.entrySet()) {
      Object value = entry.getValue();

      map.put(toLazyPrimitiveObject(entry.getKey(), keyObjectInspector),
          toLazyObject(value, valueObjectInspector));
    }

    return lazyMap;
  }

  /**
   * Convert the given object to a lazy object using the given {@link ObjectInspector}
   *
   * @param obj Object to be converted to a {@link LazyObject}
   * @param objectInspector ObjectInspector used for the conversion
   * @return the created {@link LazyObject lazy object}
   * */
  private Object toLazyUnionObject(Object obj, ObjectInspector objectInspector) {
    if (obj == null) {
      return null;
    }

    if (!(objectInspector instanceof LazyUnionObjectInspector)) {
      throw new IllegalArgumentException(
          "Invalid objectinspector found. Expected LazyUnionObjectInspector, Found "
              + objectInspector.getClass());
    }

    StandardUnion standardUnion = (StandardUnion) obj;
    LazyUnionObjectInspector lazyUnionOI = (LazyUnionObjectInspector) objectInspector;

    // Grab the tag and the field
    byte tag = standardUnion.getTag();
    Object field = standardUnion.getObject();

    ObjectInspector fieldOI = lazyUnionOI.getObjectInspectors().get(tag);

    // convert to lazy object
    Object convertedObj = null;

    if (field != null) {
      convertedObj = toLazyObject(field, fieldOI);
    }

    if (convertedObj == null) {
      return null;
    }

    return new LazyUnion(lazyUnionOI, tag, convertedObj);
  }

  /**
   * Determines if the given object is a primitive or a wrapper to a primitive. Note, even though a
   * <code>String</code> may not be a primitive in the traditional sense, but it is considered one
   * here as it is <i>not</i> a struct.
   *
   * @param clazz input class
   * @return true, if the object is a primitive or a wrapper to a primitive, false otherwise.
   * */
  private boolean isPrimitive(Class<?> clazz) {
    return clazz.isPrimitive() || ClassUtils.wrapperToPrimitive(clazz) != null
        || clazz.getSimpleName().equals("String");
  }
}
