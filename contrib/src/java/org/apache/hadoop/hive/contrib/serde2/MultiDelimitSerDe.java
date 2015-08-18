/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.contrib.serde2;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazyStruct;
import org.apache.hadoop.hive.serde2.lazy.LazyUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * This SerDe allows user to use multiple characters as the field delimiter for a table.
 * To use this SerDe, user has to specify field.delim in SERDEPROPERTIES.
 * If the table contains a column which is a collection or map, user can optionally
 * specify collection.delim or mapkey.delim as well.
 * Currently field.delim can be multiple character while collection.delim
 * and mapkey.delim should be just single character.
 */
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    serdeConstants.FIELD_DELIM, serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
    serdeConstants.SERIALIZATION_FORMAT, serdeConstants.SERIALIZATION_NULL_FORMAT,
    serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
    serdeConstants.ESCAPE_CHAR,
    serdeConstants.SERIALIZATION_ENCODING,
    LazySimpleSerDe.SERIALIZATION_EXTEND_NESTING_LEVELS})
public class MultiDelimitSerDe extends AbstractSerDe {
  private static final Log LOG = LogFactory.getLog(MultiDelimitSerDe.class.getName());
  private static final byte[] DEFAULT_SEPARATORS = {(byte) 1, (byte) 2, (byte) 3};
  // Due to HIVE-6404, define our own constant
  private static final String COLLECTION_DELIM = "collection.delim";

  private int numColumns;
  private String fieldDelimited;
  // we don't support using multiple chars as delimiters within complex types
  // collection separator
  private byte collSep;
  // map key separator
  private byte keySep;

  // The object for storing row data
  private LazyStruct cachedLazyStruct;
  //the lazy struct object inspector
  private ObjectInspector cachedObjectInspector;

  // The wrapper for byte array
  private ByteArrayRef byteArrayRef;

  private LazySimpleSerDe.SerDeParameters serdeParams = null;
  // The output stream of serialized objects
  private final ByteStream.Output serializeStream = new ByteStream.Output();
  // The Writable to return in serialize
  private final Text serializeCache = new Text();

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    // get the SerDe parameters
    serdeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, getClass().getName());

    fieldDelimited = tbl.getProperty(serdeConstants.FIELD_DELIM);
    if (fieldDelimited == null || fieldDelimited.isEmpty()) {
      throw new SerDeException("This table does not have serde property \"field.delim\"!");
    }

    // get the collection separator and map key separator
    // TODO: use serdeConstants.COLLECTION_DELIM when the typo is fixed
    collSep = LazySimpleSerDe.getByte(tbl.getProperty(COLLECTION_DELIM),
        DEFAULT_SEPARATORS[1]);
    keySep = LazySimpleSerDe.getByte(tbl.getProperty(serdeConstants.MAPKEY_DELIM),
        DEFAULT_SEPARATORS[2]);
    serdeParams.getSeparators()[1] = collSep;
    serdeParams.getSeparators()[2] = keySep;

    // Create the ObjectInspectors for the fields
    cachedObjectInspector = LazyFactory.createLazyStructInspector(serdeParams
        .getColumnNames(), serdeParams.getColumnTypes(), serdeParams
        .getSeparators(), serdeParams.getNullSequence(), serdeParams
        .isLastColumnTakesRest(), serdeParams.isEscaped(), serdeParams
        .getEscapeChar());

    cachedLazyStruct = (LazyStruct) LazyFactory.createLazyObject(cachedObjectInspector);

    assert serdeParams.getColumnNames().size() == serdeParams.getColumnTypes().size();
    numColumns = serdeParams.getColumnNames().size();
  }


  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    if (byteArrayRef == null) {
      byteArrayRef = new ByteArrayRef();
    }

    // we use the default field delimiter('\1') to replace the multiple-char field delimiter
    // but we cannot use it to parse the row since column data can contain '\1' as well
    String rowStr;
    if (blob instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) blob;
      rowStr = new String(b.getBytes());
    } else if (blob instanceof Text) {
      Text rowText = (Text) blob;
      rowStr = rowText.toString();
    } else {
      throw new SerDeException(getClass() + ": expects either BytesWritable or Text object!");
    }
    byteArrayRef.setData(rowStr.replaceAll(Pattern.quote(fieldDelimited), "\1").getBytes());
    cachedLazyStruct.init(byteArrayRef, 0, byteArrayRef.getData().length);
    // use the multi-char delimiter to parse the lazy struct
    cachedLazyStruct.parseMultiDelimit(rowStr.getBytes(), fieldDelimited.getBytes());
    return cachedLazyStruct;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    if (fields.size() != numColumns) {
      throw new SerDeException("Cannot serialize the object because there are "
          + fields.size() + " fields but the table has " + numColumns
          + " columns.");
    }

    serializeStream.reset();
    // Get all data out.
    for (int c = 0; c < numColumns; c++) {
      //write the delimiter to the stream, which means we don't need output.format.string anymore
      if (c > 0) {
        serializeStream.write(fieldDelimited.getBytes(), 0, fieldDelimited.getBytes().length);
      }

      Object field = list == null ? null : list.get(c);
      ObjectInspector fieldOI = fields.get(c).getFieldObjectInspector();

      try {
        serializeNoEncode(serializeStream, field, fieldOI, serdeParams.getSeparators(), 1,
            serdeParams.getNullSequence(), serdeParams.isEscaped(), serdeParams.getEscapeChar(),
            serdeParams.getNeedsEscape());
      } catch (IOException e) {
        throw new SerDeException(e);
      }
    }

    serializeCache.set(serializeStream.getData(), 0, serializeStream.getLength());
    return serializeCache;
  }

  // This is basically the same as LazySimpleSerDe.serialize. Except that we don't use
  // Base64 to encode binary data because we're using printable string as delimiter.
  // Consider such a row "strAQ==\1", str is a string, AQ== is the delimiter and \1
  // is the binary data.
  private static void serializeNoEncode(ByteStream.Output out, Object obj,
      ObjectInspector objInspector, byte[] separators, int level,
      Text nullSequence, boolean escaped, byte escapeChar, boolean[] needsEscape)
      throws IOException, SerDeException {
    if (obj == null) {
      out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      return;
    }

    char separator;
    List<?> list;
    switch (objInspector.getCategory()) {
      case PRIMITIVE:
        PrimitiveObjectInspector oi = (PrimitiveObjectInspector) objInspector;
        if (oi.getPrimitiveCategory() == PrimitiveCategory.BINARY) {
          BytesWritable bw = ((BinaryObjectInspector) oi).getPrimitiveWritableObject(obj);
          byte[] toWrite = new byte[bw.getLength()];
          System.arraycopy(bw.getBytes(), 0, toWrite, 0, bw.getLength());
          out.write(toWrite, 0, toWrite.length);
        } else {
          LazyUtils.writePrimitiveUTF8(out, obj, oi, escaped, escapeChar, needsEscape);
        }
        return;
      case LIST:
        separator = (char) separators[level];
        ListObjectInspector loi = (ListObjectInspector) objInspector;
        list = loi.getList(obj);
        ObjectInspector eoi = loi.getListElementObjectInspector();
        if (list == null) {
          out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              out.write(separator);
            }
            serializeNoEncode(out, list.get(i), eoi, separators, level + 1, nullSequence,
                escaped, escapeChar, needsEscape);
          }
        }
        return;
      case MAP:
        separator = (char) separators[level];
        char keyValueSeparator = (char) separators[level + 1];

        MapObjectInspector moi = (MapObjectInspector) objInspector;
        ObjectInspector koi = moi.getMapKeyObjectInspector();
        ObjectInspector voi = moi.getMapValueObjectInspector();
        Map<?, ?> map = moi.getMap(obj);
        if (map == null) {
          out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
        } else {
          boolean first = true;
          for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (first) {
              first = false;
            } else {
              out.write(separator);
            }
            serializeNoEncode(out, entry.getKey(), koi, separators, level + 2,
                nullSequence, escaped, escapeChar, needsEscape);
            out.write(keyValueSeparator);
            serializeNoEncode(out, entry.getValue(), voi, separators, level + 2,
                nullSequence, escaped, escapeChar, needsEscape);
          }
        }
        return;
      case STRUCT:
        separator = (char) separators[level];
        StructObjectInspector soi = (StructObjectInspector) objInspector;
        List<? extends StructField> fields = soi.getAllStructFieldRefs();
        list = soi.getStructFieldsDataAsList(obj);
        if (list == null) {
          out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
        } else {
          for (int i = 0; i < list.size(); i++) {
            if (i > 0) {
              out.write(separator);
            }
            serializeNoEncode(out, list.get(i), fields.get(i).getFieldObjectInspector(),
                separators, level + 1, nullSequence, escaped, escapeChar,
                needsEscape);
          }
        }
        return;
    }
    throw new RuntimeException("Unknown category type: "+ objInspector.getCategory());
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

}
