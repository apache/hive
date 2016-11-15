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

package org.apache.hadoop.hive.hbase;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

/**
 * HBaseSerDe can be used to serialize object into an HBase table and
 * deserialize objects from an HBase table.
 */
@SerDeSpec(schemaProps = {
    serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    serdeConstants.FIELD_DELIM, serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
    serdeConstants.SERIALIZATION_FORMAT, serdeConstants.SERIALIZATION_NULL_FORMAT,
    serdeConstants.SERIALIZATION_ESCAPE_CRLF,
    serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
    serdeConstants.ESCAPE_CHAR,
    serdeConstants.SERIALIZATION_ENCODING,
    LazySerDeParameters.SERIALIZATION_EXTEND_NESTING_LEVELS,
    LazySerDeParameters.SERIALIZATION_EXTEND_ADDITIONAL_NESTING_LEVELS,
    HBaseSerDe.HBASE_COLUMNS_MAPPING,
    HBaseSerDe.HBASE_TABLE_NAME,
    HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE,
    HBaseSerDe.HBASE_KEY_COL,
    HBaseSerDe.HBASE_PUT_TIMESTAMP,
    HBaseSerDe.HBASE_COMPOSITE_KEY_CLASS,
    HBaseSerDe.HBASE_COMPOSITE_KEY_TYPES,
    HBaseSerDe.HBASE_COMPOSITE_KEY_FACTORY,
    HBaseSerDe.HBASE_STRUCT_SERIALIZER_CLASS,
    HBaseSerDe.HBASE_SCAN_CACHE,
    HBaseSerDe.HBASE_SCAN_CACHEBLOCKS,
    HBaseSerDe.HBASE_SCAN_BATCH,
    HBaseSerDe.HBASE_AUTOGENERATE_STRUCT})
public class HBaseSerDe extends AbstractSerDe {
  public static final Logger LOG = LoggerFactory.getLogger(HBaseSerDe.class);

  public static final String HBASE_COLUMNS_MAPPING = "hbase.columns.mapping";
  public static final String HBASE_TABLE_NAME = "hbase.table.name";
  public static final String HBASE_TABLE_DEFAULT_STORAGE_TYPE = "hbase.table.default.storage.type";
  public static final String HBASE_KEY_COL = ":key";
  public static final String HBASE_TIMESTAMP_COL = ":timestamp";
  public static final String HBASE_PUT_TIMESTAMP = "hbase.put.timestamp";
  public static final String HBASE_COMPOSITE_KEY_CLASS = "hbase.composite.key.class";
  public static final String HBASE_COMPOSITE_KEY_TYPES = "hbase.composite.key.types";
  public static final String HBASE_COMPOSITE_KEY_FACTORY = "hbase.composite.key.factory";
  public static final String HBASE_STRUCT_SERIALIZER_CLASS = "hbase.struct.serialization.class";
  public static final String HBASE_SCAN_CACHE = "hbase.scan.cache";
  public static final String HBASE_SCAN_CACHEBLOCKS = "hbase.scan.cacheblock";
  public static final String HBASE_SCAN_BATCH = "hbase.scan.batch";
  public static final String HBASE_AUTOGENERATE_STRUCT = "hbase.struct.autogenerate";
  /**
   * Determines whether a regex matching should be done on the columns or not. Defaults to true.
   * <strong>WARNING: Note that currently this only supports the suffix wildcard .*</strong>
   */
  public static final String HBASE_COLUMNS_REGEX_MATCHING = "hbase.columns.mapping.regex.matching";
  /**
   * Defines the type for a column.
   **/
  public static final String SERIALIZATION_TYPE = "serialization.type";

  /**
   * Defines if the prefix column from hbase should be hidden.
   * It works only when @HBASE_COLUMNS_REGEX_MATCHING is true.
   * Default value of this parameter is false
   */
  public static final String HBASE_COLUMNS_PREFIX_HIDE = "hbase.columns.mapping.prefix.hide";

  private ObjectInspector cachedObjectInspector;
  private LazyHBaseRow cachedHBaseRow;

  private HBaseSerDeParameters serdeParams;
  private HBaseRowSerializer serializer;

  @Override
  public String toString() {
    return getClass() + "[" + serdeParams + "]";
  }

  public HBaseSerDe() throws SerDeException {
  }

  /**
   * Initialize the SerDe given parameters.
   * @see AbstractSerDe#initialize(Configuration, Properties)
   */
  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    serdeParams = new HBaseSerDeParameters(conf, tbl, getClass().getName());

    cachedObjectInspector =
        HBaseLazyObjectFactory.createLazyHBaseStructInspector(serdeParams, tbl);

    cachedHBaseRow = new LazyHBaseRow(
        (LazySimpleStructObjectInspector) cachedObjectInspector, serdeParams);

    serializer = new HBaseRowSerializer(serdeParams);

    if (LOG.isDebugEnabled()) {
      LOG.debug("HBaseSerDe initialized with : " + serdeParams);
    }
  }

  public static ColumnMappings parseColumnsMapping(String columnsMappingSpec)
      throws SerDeException {
    return parseColumnsMapping(columnsMappingSpec, true);
  }

  public static ColumnMappings parseColumnsMapping(
          String columnsMappingSpec, boolean doColumnRegexMatching) throws SerDeException {
	return parseColumnsMapping(columnsMappingSpec, doColumnRegexMatching, false);
  }
  /**
   * Parses the HBase columns mapping specifier to identify the column families, qualifiers
   * and also caches the byte arrays corresponding to them. One of the Hive table
   * columns maps to the HBase row key, by default the first column.
   *
   * @param columnsMappingSpec string hbase.columns.mapping specified when creating table
   * @param doColumnRegexMatching whether to do a regex matching on the columns or not
   * @param hideColumnPrefix whether to hide a prefix of column mapping in key name in a map (works only if @doColumnRegexMatching is true)
   * @return List<ColumnMapping> which contains the column mapping information by position
   * @throws org.apache.hadoop.hive.serde2.SerDeException
   */
  public static ColumnMappings parseColumnsMapping(
      String columnsMappingSpec, boolean doColumnRegexMatching, boolean hideColumnPrefix) throws SerDeException {

    if (columnsMappingSpec == null) {
      throw new SerDeException("Error: hbase.columns.mapping missing for this HBase table.");
    }

    if (columnsMappingSpec.isEmpty() || columnsMappingSpec.equals(HBASE_KEY_COL)) {
      throw new SerDeException("Error: hbase.columns.mapping specifies only the HBase table"
          + " row key. A valid Hive-HBase table must specify at least one additional column.");
    }

    int rowKeyIndex = -1;
    int timestampIndex = -1;
    List<ColumnMapping> columnsMapping = new ArrayList<ColumnMapping>();
    String[] columnSpecs = columnsMappingSpec.split(",");

    for (int i = 0; i < columnSpecs.length; i++) {
      String mappingSpec = columnSpecs[i].trim();
      String [] mapInfo = mappingSpec.split("#");
      String colInfo = mapInfo[0];

      int idxFirst = colInfo.indexOf(":");
      int idxLast = colInfo.lastIndexOf(":");

      if (idxFirst < 0 || !(idxFirst == idxLast)) {
        throw new SerDeException("Error: the HBase columns mapping contains a badly formed " +
            "column family, column qualifier specification.");
      }

      ColumnMapping columnMapping = new ColumnMapping();

      if (colInfo.equals(HBASE_KEY_COL)) {
        rowKeyIndex = i;
        columnMapping.familyName = colInfo;
        columnMapping.familyNameBytes = Bytes.toBytes(colInfo);
        columnMapping.qualifierName = null;
        columnMapping.qualifierNameBytes = null;
        columnMapping.hbaseRowKey = true;
      } else if (colInfo.equals(HBASE_TIMESTAMP_COL)) {
        timestampIndex = i;
        columnMapping.familyName = colInfo;
        columnMapping.familyNameBytes = Bytes.toBytes(colInfo);
        columnMapping.qualifierName = null;
        columnMapping.qualifierNameBytes = null;
        columnMapping.hbaseTimestamp = true;
      } else {
        String [] parts = colInfo.split(":");
        assert(parts.length > 0 && parts.length <= 2);
        columnMapping.familyName = parts[0];
        columnMapping.familyNameBytes = Bytes.toBytes(parts[0]);
        columnMapping.hbaseRowKey = false;
        columnMapping.hbaseTimestamp = false;

        if (parts.length == 2) {

          if (doColumnRegexMatching && parts[1].endsWith(".*")) {
            // we have a prefix with a wildcard
            columnMapping.qualifierPrefix = parts[1].substring(0, parts[1].length() - 2);
            columnMapping.qualifierPrefixBytes = Bytes.toBytes(columnMapping.qualifierPrefix);
            //pass a flag to hide prefixes
            columnMapping.doPrefixCut=hideColumnPrefix;
            // we weren't provided any actual qualifier name. Set these to
            // null.
            columnMapping.qualifierName = null;
            columnMapping.qualifierNameBytes = null;
          } else {
            // set the regular provided qualifier names
            columnMapping.qualifierName = parts[1];
            columnMapping.qualifierNameBytes = Bytes.toBytes(parts[1]);
            //if there is no prefix then we don't cut anything
            columnMapping.doPrefixCut=false;
          }
        } else {
          columnMapping.qualifierName = null;
          columnMapping.qualifierNameBytes = null;
        }
      }

      columnMapping.mappingSpec = mappingSpec;

      columnsMapping.add(columnMapping);
    }

    if (rowKeyIndex == -1) {
      rowKeyIndex = 0;
      ColumnMapping columnMapping = new ColumnMapping();
      columnMapping.familyName = HBaseSerDe.HBASE_KEY_COL;
      columnMapping.familyNameBytes = Bytes.toBytes(HBaseSerDe.HBASE_KEY_COL);
      columnMapping.qualifierName = null;
      columnMapping.qualifierNameBytes = null;
      columnMapping.hbaseRowKey = true;
      columnMapping.mappingSpec = HBaseSerDe.HBASE_KEY_COL;
      columnsMapping.add(0, columnMapping);
    }

    return new ColumnMappings(columnsMapping, rowKeyIndex, timestampIndex);
  }

  public LazySerDeParameters getSerdeParams() {
    return serdeParams.getSerdeParams();
  }

  public HBaseSerDeParameters getHBaseSerdeParam() {
    return serdeParams;
  }

  /**
   * Deserialize a row from the HBase Result writable to a LazyObject
   * @param result the HBase Result Writable containing the row
   * @return the deserialized object
   * @see AbstractSerDe#deserialize(Writable)
   */
  @Override
  public Object deserialize(Writable result) throws SerDeException {
    if (!(result instanceof ResultWritable)) {
      throw new SerDeException(getClass().getName() + ": expects ResultWritable!");
    }

    cachedHBaseRow.init(((ResultWritable) result).getResult());

    return cachedHBaseRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return PutWritable.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    try {
      return serializer.serialize(obj, objInspector);
    } catch (SerDeException e) {
      throw e;
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  public HBaseKeyFactory getKeyFactory() {
    return serdeParams.getKeyFactory();
  }

  public static void configureJobConf(TableDesc tableDesc, JobConf jobConf) throws Exception {
    HBaseSerDeParameters serdeParams =
        new HBaseSerDeParameters(jobConf, tableDesc.getProperties(), HBaseSerDe.class.getName());
    serdeParams.getKeyFactory().configureJobConf(tableDesc, jobConf);
  }
}
