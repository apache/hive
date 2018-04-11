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

package org.apache.hive.hcatalog.streaming;


import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Streaming Writer handles delimited input (eg. CSV).
 * Delimited input is parsed & reordered to match column order in table
 * Uses Lazy Simple Serde to process delimited input
 */
public class DelimitedInputWriter extends AbstractRecordWriter {
  private final boolean reorderingNeeded;
  private String delimiter;
  private char serdeSeparator;
  private int[] fieldToColMapping;
  private final ArrayList<String> tableColumns;
  private LazySimpleSerDe serde = null;

  private final LazySimpleStructObjectInspector recordObjInspector;
  private final ObjectInspector[] bucketObjInspectors;
  private final StructField[] bucketStructFields;

  static final private Logger LOG = LoggerFactory.getLogger(DelimitedInputWriter.class.getName());

  /** Constructor. Uses default separator of the LazySimpleSerde
   * @param colNamesForFields Column name assignment for input fields. nulls or empty
   *                          strings in the array indicates the fields to be skipped
   * @param delimiter input field delimiter
   * @param endPoint Hive endpoint
   * @throws ConnectionError Problem talking to Hive
   * @throws ClassNotFoundException Serde class not found
   * @throws SerializationError Serde initialization/interaction failed
   * @throws StreamingException Problem acquiring file system path for partition
   * @throws InvalidColumn any element in colNamesForFields refers to a non existing column
   */
  public DelimitedInputWriter(String[] colNamesForFields, String delimiter,
                              HiveEndPoint endPoint, StreamingConnection conn)
    throws ClassNotFoundException, ConnectionError, SerializationError,
      InvalidColumn, StreamingException {
    this(colNamesForFields, delimiter, endPoint, null, conn);
  }
 /** Constructor. Uses default separator of the LazySimpleSerde
  * @param colNamesForFields Column name assignment for input fields. nulls or empty
  *                          strings in the array indicates the fields to be skipped
  * @param delimiter input field delimiter
  * @param endPoint Hive endpoint
  * @param conf a Hive conf object. Can be null if not using advanced hive settings.
  * @throws ConnectionError Problem talking to Hive
  * @throws ClassNotFoundException Serde class not found
  * @throws SerializationError Serde initialization/interaction failed
  * @throws StreamingException Problem acquiring file system path for partition
  * @throws InvalidColumn any element in colNamesForFields refers to a non existing column
  */
   public DelimitedInputWriter(String[] colNamesForFields, String delimiter,
                              HiveEndPoint endPoint, HiveConf conf, StreamingConnection conn)
          throws ClassNotFoundException, ConnectionError, SerializationError,
                 InvalidColumn, StreamingException {
     this(colNamesForFields, delimiter, endPoint, conf,
       (char) LazySerDeParameters.DefaultSeparators[0], conn);
   }
  /**
   * Constructor. Allows overriding separator of the LazySimpleSerde
   * @param colNamesForFields Column name assignment for input fields
   * @param delimiter input field delimiter
   * @param endPoint Hive endpoint
   * @param conf a Hive conf object. Set to null if not using advanced hive settings.
   * @param serdeSeparator separator used when encoding data that is fed into the
   *                             LazySimpleSerde. Ensure this separator does not occur
   *                             in the field data
   * @param conn connection this Writer is to be used with
   * @throws ConnectionError Problem talking to Hive
   * @throws ClassNotFoundException Serde class not found
   * @throws SerializationError Serde initialization/interaction failed
   * @throws StreamingException Problem acquiring file system path for partition
   * @throws InvalidColumn any element in colNamesForFields refers to a non existing column
   */
  public DelimitedInputWriter(String[] colNamesForFields, String delimiter,
                              HiveEndPoint endPoint, HiveConf conf, char serdeSeparator, StreamingConnection conn)
          throws ClassNotFoundException, ConnectionError, SerializationError,
                 InvalidColumn, StreamingException {
    super(endPoint, conf, conn);
    this.tableColumns = getCols(tbl);
    this.serdeSeparator = serdeSeparator;
    this.delimiter = delimiter;
    this.fieldToColMapping = getFieldReordering(colNamesForFields, getTableColumns());
    this.reorderingNeeded = isReorderingNeeded(delimiter, getTableColumns());
    LOG.debug("Field reordering needed = " + this.reorderingNeeded + ", for endpoint " + endPoint);
    this.serdeSeparator = serdeSeparator;
    this.serde = createSerde(tbl, conf, serdeSeparator);

    // get ObjInspectors for entire record and bucketed cols
    try {
      this.recordObjInspector = (LazySimpleStructObjectInspector) serde.getObjectInspector();
      this.bucketObjInspectors = getObjectInspectorsForBucketedCols(bucketIds, recordObjInspector);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to get ObjectInspector for bucket columns", e);
    }

    // get StructFields for bucketed cols
    bucketStructFields = new StructField[bucketIds.size()];
    List<? extends StructField> allFields = recordObjInspector.getAllStructFieldRefs();
    for (int i = 0; i < bucketIds.size(); i++) {
      bucketStructFields[i] = allFields.get(bucketIds.get(i));
    }
  }
  /**
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #DelimitedInputWriter(String[], String, HiveEndPoint, StreamingConnection)}
   */
  public DelimitedInputWriter(String[] colNamesForFields, String delimiter,
                              HiveEndPoint endPoint)
    throws ClassNotFoundException, ConnectionError, SerializationError,
    InvalidColumn, StreamingException {
    this(colNamesForFields, delimiter, endPoint, null, null);
  }
  /**
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #DelimitedInputWriter(String[], String, HiveEndPoint, HiveConf, StreamingConnection)}
   */
  public DelimitedInputWriter(String[] colNamesForFields, String delimiter,
                              HiveEndPoint endPoint, HiveConf conf)
    throws ClassNotFoundException, ConnectionError, SerializationError,
    InvalidColumn, StreamingException {
    this(colNamesForFields, delimiter, endPoint, conf,
      (char) LazySerDeParameters.DefaultSeparators[0], null);
  }
  /**
   * @deprecated As of release 1.3/2.1.  Replaced by {@link #DelimitedInputWriter(String[], String, HiveEndPoint, HiveConf, char, StreamingConnection)}
   */
  public DelimitedInputWriter(String[] colNamesForFields, String delimiter,
                              HiveEndPoint endPoint, HiveConf conf, char serdeSeparator)
    throws ClassNotFoundException, StreamingException {
    this(colNamesForFields, delimiter, endPoint, conf, serdeSeparator, null);
  }

  private boolean isReorderingNeeded(String delimiter, ArrayList<String> tableColumns) {
    return !( delimiter.equals(String.valueOf(getSerdeSeparator()))
            && areFieldsInColOrder(fieldToColMapping)
            && tableColumns.size()>=fieldToColMapping.length );
  }

  private static boolean areFieldsInColOrder(int[] fieldToColMapping) {
    for(int i=0; i<fieldToColMapping.length; ++i) {
      if(fieldToColMapping[i]!=i) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  static int[] getFieldReordering(String[] colNamesForFields, List<String> tableColNames)
          throws InvalidColumn {
    int[] result = new int[ colNamesForFields.length ];
    for(int i=0; i<colNamesForFields.length; ++i) {
      result[i] = -1;
    }
    int i=-1, fieldLabelCount=0;
    for( String col : colNamesForFields ) {
      ++i;
      if(col == null) {
        continue;
      }
      if( col.trim().isEmpty() ) {
        continue;
      }
      ++fieldLabelCount;
      int loc = tableColNames.indexOf(col);
      if(loc == -1) {
        throw new InvalidColumn("Column '" + col + "' not found in table for input field " + i+1);
      }
      result[i] = loc;
    }
    if(fieldLabelCount>tableColNames.size()) {
      throw new InvalidColumn("Number of field names exceeds the number of columns in table");
    }
    return result;
  }

  // Reorder fields in record based on the order of columns in the table
  protected byte[] reorderFields(byte[] record) throws UnsupportedEncodingException {
    if(!reorderingNeeded) {
      return record;
    }
    String[] reorderedFields = new String[getTableColumns().size()];
    String decoded = new String(record);
    String[] fields = decoded.split(delimiter,-1);
    for (int i=0; i<fieldToColMapping.length; ++i) {
      int newIndex = fieldToColMapping[i];
      if(newIndex != -1) {
        reorderedFields[newIndex] = fields[i];
      }
    }
    return join(reorderedFields, getSerdeSeparator());
  }

  // handles nulls in items[]
  // TODO: perhaps can be made more efficient by creating a byte[] directly
  private static byte[] join(String[] items, char separator) {
    StringBuilder buff = new StringBuilder(100);
    if(items.length == 0)
      return "".getBytes();
    int i=0;
    for(; i<items.length-1; ++i) {
      if(items[i]!=null) {
        buff.append(items[i]);
      }
      buff.append(separator);
    }
    if(items[i]!=null) {
      buff.append(items[i]);
    }
    return buff.toString().getBytes();
  }

  protected ArrayList<String> getTableColumns() {
    return tableColumns;
  }

  @Override
  public void write(long writeId, byte[] record)
          throws SerializationError, StreamingIOFailure {
    try {
      byte[] orderedFields = reorderFields(record);
      Object encodedRow = encode(orderedFields);
      int bucket = getBucket(encodedRow);
      getRecordUpdater(bucket).insert(writeId, encodedRow);
    } catch (IOException e) {
      throw new StreamingIOFailure("Error writing record in transaction write id ("
              + writeId + ")", e);
    }
  }

  @Override
  public AbstractSerDe getSerde() {
    return serde;
  }

  protected LazySimpleStructObjectInspector getRecordObjectInspector() {
    return recordObjInspector;
  }

  @Override
  protected StructField[] getBucketStructFields() {
    return bucketStructFields;
  }

  protected ObjectInspector[] getBucketObjectInspectors() {
    return bucketObjInspectors;
  }

  @Override
  public Object encode(byte[] record) throws SerializationError {
    try {
      BytesWritable blob = new BytesWritable();
      blob.set(record, 0, record.length);
      return serde.deserialize(blob);
    } catch (SerDeException e) {
      throw new SerializationError("Unable to convert byte[] record into Object", e);
    }
  }

  /**
   * Creates LazySimpleSerde
   * @return
   * @throws SerializationError if serde could not be initialized
   * @param tbl
   */
  protected static LazySimpleSerDe createSerde(Table tbl, HiveConf conf, char serdeSeparator)
          throws SerializationError {
    try {
      Properties tableProps = MetaStoreUtils.getTableMetadata(tbl);
      tableProps.setProperty("field.delim", String.valueOf(serdeSeparator));
      LazySimpleSerDe serde = new LazySimpleSerDe();
      SerDeUtils.initializeSerDe(serde, conf, tableProps, null);
      return serde;
    } catch (SerDeException e) {
      throw new SerializationError("Error initializing serde", e);
    }
  }

  private ArrayList<String> getCols(Table table) {
    List<FieldSchema> cols = table.getSd().getCols();
    ArrayList<String> colNames = new ArrayList<String>(cols.size());
    for (FieldSchema col : cols) {
      colNames.add(col.getName().toLowerCase());
    }
    return  colNames;
  }

  public char getSerdeSeparator() {
    return serdeSeparator;
  }
}
