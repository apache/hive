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

package org.apache.hadoop.hive.llap;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.hive.llap.TypeDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Row-based record reader for LLAP.
 */
public class LlapRowRecordReader implements RecordReader<NullWritable, Row> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapRowRecordReader.class);

  protected final Configuration conf;
  protected final RecordReader<NullWritable, Text> reader;
  protected final Schema schema;
  protected final AbstractSerDe serde;
  protected final Text textData = new Text();

  public LlapRowRecordReader(Configuration conf, Schema schema, RecordReader<NullWritable, Text> reader) throws IOException {
    this.conf = conf;
    this.schema = schema;
    this.reader = reader;

    try {
      serde = initSerDe(conf);
    } catch (SerDeException err) {
      throw new IOException(err);
    }
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public Row createValue() {
    return new Row(schema);
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  @Override
  public boolean next(NullWritable key, Row value) throws IOException {
    Preconditions.checkArgument(value != null);

    boolean hasNext = reader.next(key,  textData);
    if (hasNext) {
      // Deserialize Text to column values, and populate the row record
      Object rowObj;
      try {
        StructObjectInspector rowOI = (StructObjectInspector) serde.getObjectInspector();
        rowObj = serde.deserialize(textData);
        List<? extends StructField> colFields = rowOI.getAllStructFieldRefs();
        for (int idx = 0; idx < colFields.size(); ++idx) {
          StructField field = colFields.get(idx);
          Object colValue = rowOI.getStructFieldData(rowObj, field);
          Preconditions.checkState(field.getFieldObjectInspector().getCategory() == Category.PRIMITIVE,
              "Cannot handle non-primitive column type " + field.getFieldObjectInspector().getTypeName());

          PrimitiveObjectInspector poi = (PrimitiveObjectInspector) field.getFieldObjectInspector();
          // char/varchar special cased here since the row record handles them using Text
          switch (poi.getPrimitiveCategory()) {
            case CHAR:
              value.setValue(idx, ((HiveCharWritable) poi.getPrimitiveWritableObject(colValue)).getPaddedValue());
              break;
            case VARCHAR:
              value.setValue(idx, ((HiveVarcharWritable) poi.getPrimitiveWritableObject(colValue)).getTextValue());
              break;
            default:
              value.setValue(idx, (Writable) poi.getPrimitiveWritableObject(colValue));
              break;
          }
        }
      } catch (SerDeException err) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Error deserializing row from text: " + textData);
        }
        throw new IOException("Error deserializing row data", err);
      }
    }

    return hasNext;
  }

  public Schema getSchema() {
    return schema;
  }

  protected AbstractSerDe initSerDe(Configuration conf) throws SerDeException {
    Properties props = new Properties();
    StringBuffer columnsBuffer = new StringBuffer();
    StringBuffer typesBuffer = new StringBuffer();
    boolean isFirst = true;
    for (FieldDesc colDesc : schema.getColumns()) {
      if (!isFirst) {
        columnsBuffer.append(',');
        typesBuffer.append(',');
      }
      columnsBuffer.append(colDesc.getName());
      typesBuffer.append(colDesc.getTypeDesc().toString());
      isFirst = false;
    }
    String columns = columnsBuffer.toString();
    String types = typesBuffer.toString();
    props.put(serdeConstants.LIST_COLUMNS, columns);
    props.put(serdeConstants.LIST_COLUMN_TYPES, types);
    props.put(serdeConstants.ESCAPE_CHAR, "\\");
    AbstractSerDe serde = new LazySimpleSerDe();
    serde.initialize(conf, props);

    return serde;
  }
}
