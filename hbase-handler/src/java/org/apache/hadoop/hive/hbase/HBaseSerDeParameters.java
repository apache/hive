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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.hbase.ColumnMappings.ColumnMapping;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.SerDeParameters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * HBaseSerDeParameters encapsulates SerDeParameters and additional configurations that are specific for
 * HBaseSerDe.
 *
 */
public class HBaseSerDeParameters {

  private final SerDeParameters serdeParams;

  private final Configuration job;
  private final Properties tbl;

  private final String columnMappingString;
  private final ColumnMappings columnMappings;
  private final boolean doColumnRegexMatching;

  private final long putTimestamp;
  private final HBaseKeyFactory keyFactory;

  HBaseSerDeParameters(Configuration job, Properties tbl, String serdeName) throws SerDeException {
    this.job = job;
    this.tbl = tbl;
    this.serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, serdeName);
    this.putTimestamp = Long.valueOf(tbl.getProperty(HBaseSerDe.HBASE_PUT_TIMESTAMP, "-1"));

    // Read configuration parameters
    columnMappingString = tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING);
    doColumnRegexMatching = Boolean.valueOf(tbl.getProperty(HBaseSerDe.HBASE_COLUMNS_REGEX_MATCHING, "true"));
    // Parse and initialize the HBase columns mapping
    columnMappings = HBaseSerDe.parseColumnsMapping(columnMappingString, doColumnRegexMatching);
    columnMappings.setHiveColumnDescription(serdeName, serdeParams.getColumnNames(), serdeParams.getColumnTypes());

    // Precondition: make sure this is done after the rest of the SerDe initialization is done.
    String hbaseTableStorageType = tbl.getProperty(HBaseSerDe.HBASE_TABLE_DEFAULT_STORAGE_TYPE);
    columnMappings.parseColumnStorageTypes(hbaseTableStorageType);

    // Build the type property string if not supplied
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    if (columnTypeProperty == null) {
      tbl.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnMappings.toTypesString());
    }

    this.keyFactory = initKeyFactory(job, tbl);
  }

  private HBaseKeyFactory initKeyFactory(Configuration conf, Properties tbl) throws SerDeException {
    try {
      HBaseKeyFactory keyFactory = createKeyFactory(conf, tbl);
      if (keyFactory != null) {
        keyFactory.init(this, tbl);
      }
      return keyFactory;
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  private static HBaseKeyFactory createKeyFactory(Configuration job, Properties tbl) throws Exception {
    String factoryClassName = tbl.getProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_FACTORY);
    if (factoryClassName != null) {
      Class<?> factoryClazz = Class.forName(factoryClassName);
      return (HBaseKeyFactory) ReflectionUtils.newInstance(factoryClazz, job);
    }
    String keyClassName = tbl.getProperty(HBaseSerDe.HBASE_COMPOSITE_KEY_CLASS);
    if (keyClassName != null) {
      Class<?> keyClass = Class.forName(keyClassName);
      return new CompositeHBaseKeyFactory(keyClass);
    }
    return new DefaultHBaseKeyFactory();
  }

  public List<String> getColumnNames() {
    return serdeParams.getColumnNames();
  }

  public List<TypeInfo> getColumnTypes() {
    return serdeParams.getColumnTypes();
  }

  public SerDeParameters getSerdeParams() {
    return serdeParams;
  }

  public long getPutTimestamp() {
    return putTimestamp;
  }

  public int getKeyIndex() {
    return columnMappings.getKeyIndex();
  }

  public ColumnMapping getKeyColumnMapping() {
    return columnMappings.getKeyMapping();
  }

  public ColumnMappings getColumnMappings() {
    return columnMappings;
  }

  public HBaseKeyFactory getKeyFactory() {
    return keyFactory;
  }

  public Configuration getBaseConfiguration() {
    return job;
  }

  public TypeInfo getTypeForName(String columnName) {
    List<String> columnNames = serdeParams.getColumnNames();
    List<TypeInfo> columnTypes = serdeParams.getColumnTypes();
    for (int i = 0; i < columnNames.size(); i++) {
      if (columnName.equals(columnNames.get(i))) {
        return columnTypes.get(i);
      }
    }
    throw new IllegalArgumentException("Invalid column name " + columnName);
  }

  public String toString() {
    return "[" + columnMappingString + ":" + getColumnNames() + ":" + getColumnTypes() + "]";
  }
}
