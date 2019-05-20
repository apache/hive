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

package org.apache.hadoop.hive.serde2.thrift;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.service.rpc.thrift.TColumn;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This SerDe is used to serialize the final output to thrift-able objects directly in the SerDe. Use this SerDe only for final output resultSets.
 * It is used  if HIVE_SERVER2_THRIFT_RESULTSET_SERIALIZE_IN_TASKS is set to true. It buffers rows that come in from FileSink till it reaches max_buffer_size (also configurable)
 * or all rows are finished and FileSink.closeOp() is called.
 */
public class ThriftJDBCBinarySerDe extends AbstractSerDe {
  public static final Logger LOG = LoggerFactory.getLogger(ThriftJDBCBinarySerDe.class.getName());
  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private ColumnBuffer[] columnBuffers;
  private TypeInfo rowTypeInfo;
  private ArrayList<Object> row;
  private BytesWritable serializedBytesWritable = new BytesWritable();
  private ByteStream.Output output = new ByteStream.Output();
  private TProtocol protocol = new TCompactProtocol(new TIOStreamTransport(output));
  private ThriftFormatter thriftFormatter = new ThriftFormatter();
  private int MAX_BUFFERED_ROWS;
  private int count;
  private StructObjectInspector rowObjectInspector;


  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    // Get column names
    MAX_BUFFERED_ROWS =
      HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_DEFAULT_FETCH_SIZE);
    LOG.info("ThriftJDBCBinarySerDe max number of buffered columns: " + MAX_BUFFERED_ROWS);
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    final String columnNameDelimiter = tbl.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? tbl
        .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    rowObjectInspector =
        (StructObjectInspector) TypeInfoUtils
            .getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);

    initializeRowAndColumns();
    try {
      thriftFormatter.initialize(conf, tbl);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  private Writable serializeBatch() throws SerDeException {
	  output.reset();
	  for (int i = 0; i < columnBuffers.length; i++) {
		  TColumn tColumn = columnBuffers[i].toTColumn();
		  try {
			  tColumn.write(protocol);
		  } catch(TException e) {
			  throw new SerDeException(e);
		  }
	  }
	  initializeRowAndColumns();
	  serializedBytesWritable.set(output.getData(), 0, output.getLength());
	  return serializedBytesWritable;
  }

  // use the columnNames to initialize the reusable row object and the columnBuffers. reason this is being done is if buffer is full, we should reinitialize the
  // column buffers, otherwise at the end when closeOp() is called, things get printed multiple times.
  private void initializeRowAndColumns() {
	    row = new ArrayList<Object>(columnNames.size());
	    for (int i = 0; i < columnNames.size(); i++) {
	      row.add(null);
	    }
	    // Initialize column buffers
	    columnBuffers = new ColumnBuffer[columnNames.size()];
	    for (int i = 0; i < columnBuffers.length; i++) {
	      columnBuffers[i] = new ColumnBuffer(Type.getType(columnTypes.get(i)));
	    }
  }

  /**
   * Write TColumn objects to the underlying stream of TProtocol
   */
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    //if row is null, it means there are no more rows (closeOp()). another case can be that the buffer is full.
    if (obj == null)
        return serializeBatch();
    count += 1;
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    try {
	    Object[] formattedRow = (Object[]) thriftFormatter.convert(obj, objInspector);
	    for (int i = 0; i < columnNames.size(); i++) {
	        columnBuffers[i].addValue(formattedRow[i]);
	    }
    } catch (Exception e) {
        throw new SerDeException(e);
    }
    if (count == MAX_BUFFERED_ROWS) {
        count = 0;
        return serializeBatch();
    }
    return null;
  }

  @Override
  public SerDeStats getSerDeStats() {
    return null;
  }

  /**
   * Return the bytes from this writable blob.
   * Eventually the client of this method will interpret the byte using the Thrift Protocol
   */
  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    return ((BytesWritable) blob).getBytes();
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowObjectInspector;
  }

}
