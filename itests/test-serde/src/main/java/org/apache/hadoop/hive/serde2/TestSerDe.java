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

package org.apache.hadoop.hive.serde2;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.MetadataListStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

/**
 * TestSerDe.
 *
 */
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    TestSerDe.COLUMNS, TestSerDe.COLUMNS_COMMENTS, TestSerDe.DEFAULT_SERIALIZATION_FORMAT})
public class TestSerDe extends AbstractSerDe {

  public static final Logger LOG = LoggerFactory.getLogger(TestSerDe.class.getName());

  public static final String COLUMNS = "columns";
  public static final String COLUMNS_COMMENTS = "columns.comments";
  public static final String DEFAULT_SERIALIZATION_FORMAT = "testserde.default.serialization.format";

  public String getShortName() {
    return shortName();
  }

  public static String shortName() {
    return "test_meta";
  }

  public static final String DefaultSeparator = "\002";

  private String separator;
  // constant for now, will make it configurable later.
  private final String nullString = "\\N";
  private List<String> columnNames;
  private ObjectInspector cachedObjectInspector;

  @Override
  public String toString() {
    return "TestSerDe[" + separator + "," + columnNames + "]";
  }

  public TestSerDe() throws SerDeException {
    separator = DefaultSeparator;
  }

  @Override
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    separator = DefaultSeparator;
    String altSep = tbl.getProperty(DEFAULT_SERIALIZATION_FORMAT);
    if (altSep != null && altSep.length() > 0) {
      try {
        byte[] b = new byte[1];
        b[0] = Byte.valueOf(altSep).byteValue();
        separator = new String(b);
      } catch (NumberFormatException e) {
        separator = altSep;
      }
    }

    String columnProperty = tbl.getProperty(COLUMNS);
    if (columnProperty == null || columnProperty.length() == 0) {
      // Hack for tables with no columns
      // Treat it as a table with a single column called "col"
      cachedObjectInspector = ObjectInspectorFactory
          .getReflectionObjectInspector(ColumnSet.class,
          ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    } else {
      columnNames = Arrays.asList(columnProperty.split(","));
      cachedObjectInspector = MetadataListStructObjectInspector
          .getInstance(columnNames,Lists.newArrayList(Splitter.on('\0').split(tbl.getProperty(COLUMNS_COMMENTS))));
    }
    LOG.info(getClass().getName() + ": initialized with columnNames: "
        + columnNames);
  }

  public static Object deserialize(ColumnSet c, String row, String sep,
      String nullString) throws Exception {
    if (c.col == null) {
      c.col = new ArrayList<String>();
    } else {
      c.col.clear();
    }
    String[] l1 = row.split(sep, -1);

    for (String s : l1) {
      if (s.equals(nullString)) {
        c.col.add(null);
      } else {
        c.col.add(s);
      }
    }
    return (c);
  }

  ColumnSet deserializeCache = new ColumnSet();

  @Override
  public Object deserialize(Writable field) throws SerDeException {
    String row = null;
    if (field instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) field;
      try {
        row = Text.decode(b.get(), 0, b.getSize());
      } catch (CharacterCodingException e) {
        throw new SerDeException(e);
      }
    } else if (field instanceof Text) {
      row = field.toString();
    }
    try {
      deserialize(deserializeCache, row, separator, nullString);
      if (columnNames != null) {
        assert (columnNames.size() == deserializeCache.col.size());
      }
      return deserializeCache;
    } catch (ClassCastException e) {
      throw new SerDeException(this.getClass().getName()
          + " expects Text or BytesWritable", e);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  Text serializeCache = new Text();

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {

    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(separator);
      }
      Object column = soi.getStructFieldData(obj, fields.get(i));
      if (fields.get(i).getFieldObjectInspector().getCategory() == Category.PRIMITIVE) {
        // For primitive object, serialize to plain string
        sb.append(column == null ? nullString : column.toString());
      } else {
        // For complex object, serialize to JSON format
        sb.append(SerDeUtils.getJSONString(column, fields.get(i)
            .getFieldObjectInspector()));
      }
    }
    serializeCache.set(sb.toString());
    return serializeCache;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

}
