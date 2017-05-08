/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.serde;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloConnectionParameters;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapper;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloRowIdColumnMapping;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 *
 */
public class AccumuloSerDeParameters extends AccumuloConnectionParameters {
  private static final Logger log = LoggerFactory.getLogger(AccumuloSerDeParameters.class);

  public static final String COLUMN_MAPPINGS = "accumulo.columns.mapping";
  public static final String ITERATOR_PUSHDOWN_KEY = "accumulo.iterator.pushdown";
  public static final boolean ITERATOR_PUSHDOWN_DEFAULT = true;

  public static final String DEFAULT_STORAGE_TYPE = "accumulo.default.storage";

  public static final String VISIBILITY_LABEL_KEY = "accumulo.visibility.label";
  public static final ColumnVisibility DEFAULT_VISIBILITY_LABEL = new ColumnVisibility();

  public static final String AUTHORIZATIONS_KEY = "accumulo.authorizations";

  public static final String COMPOSITE_ROWID_FACTORY = "accumulo.composite.rowid.factory";
  public static final String COMPOSITE_ROWID_CLASS = "accumulo.composite.rowid";

  protected final ColumnMapper columnMapper;

  private Properties tableProperties;
  private String serdeName;
  private LazySerDeParameters lazySerDeParameters;
  private AccumuloRowIdFactory rowIdFactory;

  public AccumuloSerDeParameters(Configuration conf, Properties tableProperties, String serdeName)
      throws SerDeException {
    super(conf);
    this.tableProperties = tableProperties;
    this.serdeName = serdeName;

    lazySerDeParameters = new LazySerDeParameters(conf, tableProperties, serdeName);

    // The default encoding for this table when not otherwise specified
    String defaultStorage = tableProperties.getProperty(DEFAULT_STORAGE_TYPE);

    columnMapper = new ColumnMapper(getColumnMappingValue(), defaultStorage,
        lazySerDeParameters.getColumnNames(), lazySerDeParameters.getColumnTypes());

    log.info("Constructed column mapping " + columnMapper);

    // Generate types for column mapping
    if (null == getColumnTypeValue()) {
      tableProperties.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnMapper.getTypesString());
    }

    if (columnMapper.size() < lazySerDeParameters.getColumnNames().size()) {
      throw new TooManyHiveColumnsException("You have more " + COLUMN_MAPPINGS
          + " fields than hive columns");
    } else if (columnMapper.size() > lazySerDeParameters.getColumnNames().size()) {
      throw new TooManyAccumuloColumnsException(
          "You have more hive columns than fields mapped with " + COLUMN_MAPPINGS);
    }

    this.rowIdFactory = initRowIdFactory(conf, tableProperties);
  }

  protected AccumuloRowIdFactory initRowIdFactory(Configuration conf, Properties tbl)
      throws SerDeException {
    try {
      AccumuloRowIdFactory keyFactory = createRowIdFactory(conf, tbl);
      if (keyFactory != null) {
        keyFactory.init(this, tbl);
      }
      return keyFactory;
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  protected AccumuloRowIdFactory createRowIdFactory(Configuration job, Properties tbl)
      throws Exception {
    // Try to load the composite factory if one was provided
    String factoryClassName = tbl.getProperty(COMPOSITE_ROWID_FACTORY);
    if (factoryClassName != null) {
      log.info("Loading CompositeRowIdFactory class " + factoryClassName);
      Class<?> factoryClazz = JavaUtils.loadClass(factoryClassName);
      return (AccumuloRowIdFactory) ReflectionUtils.newInstance(factoryClazz, job);
    }

    // See if a custom CompositeKey class was provided
    String keyClassName = tbl.getProperty(COMPOSITE_ROWID_CLASS);
    if (keyClassName != null) {
      log.info("Loading CompositeRowId class " + keyClassName);
      Class<?> keyClass = JavaUtils.loadClass(keyClassName);
      Class<? extends AccumuloCompositeRowId> compositeRowIdClass = keyClass
          .asSubclass(AccumuloCompositeRowId.class);
      return new CompositeAccumuloRowIdFactory(compositeRowIdClass);
    }

    return new DefaultAccumuloRowIdFactory();
  }

  public LazySerDeParameters getSerDeParameters() {
    return lazySerDeParameters;
  }

  public Properties getTableProperties() {
    return tableProperties;
  }

  public String getColumnTypeValue() {
    return tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
  }

  public String getSerDeName() {
    return serdeName;
  }

  public String getColumnMappingValue() {
    return tableProperties.getProperty(COLUMN_MAPPINGS);
  }

  public HiveAccumuloRowIdColumnMapping getRowIdColumnMapping() {
    return columnMapper.getRowIdMapping();
  }

  public boolean getIteratorPushdown() {
    return conf.getBoolean(ITERATOR_PUSHDOWN_KEY, ITERATOR_PUSHDOWN_DEFAULT);
  }

  public List<String> getHiveColumnNames() {
    return Collections.unmodifiableList(lazySerDeParameters.getColumnNames());
  }

  public List<TypeInfo> getHiveColumnTypes() {
    return Collections.unmodifiableList(lazySerDeParameters.getColumnTypes());
  }

  public ColumnMapper getColumnMapper() {
    return columnMapper;
  }

  public int getRowIdOffset() {
    return columnMapper.getRowIdOffset();
  }

  public List<ColumnMapping> getColumnMappings() {
    return columnMapper.getColumnMappings();
  }

  public AccumuloRowIdFactory getRowIdFactory() {
    return rowIdFactory;
  }

  public String getRowIdHiveColumnName() {
    int rowIdOffset = columnMapper.getRowIdOffset();
    if (-1 == rowIdOffset) {
      return null;
    }

    List<String> hiveColumnNames = lazySerDeParameters.getColumnNames();
    if (0 > rowIdOffset || hiveColumnNames.size() <= rowIdOffset) {
      throw new IllegalStateException("Tried to find rowID offset at position " + rowIdOffset
          + " from Hive columns " + hiveColumnNames);
    }

    return hiveColumnNames.get(rowIdOffset);
  }

  public ColumnMapping getColumnMappingForHiveColumn(String hiveColumn) {
    List<String> hiveColumnNames = lazySerDeParameters.getColumnNames();

    for (int offset = 0; offset < hiveColumnNames.size() && offset < columnMapper.size(); offset++) {
      String hiveColumnName = hiveColumnNames.get(offset);
      if (hiveColumn.equals(hiveColumnName)) {
        return columnMapper.get(offset);
      }
    }

    throw new NoSuchElementException("Could not find column mapping for Hive column " + hiveColumn);
  }

  public TypeInfo getTypeForHiveColumn(String hiveColumn) {
    List<String> hiveColumnNames = lazySerDeParameters.getColumnNames();
    List<TypeInfo> hiveColumnTypes = lazySerDeParameters.getColumnTypes();

    for (int i = 0; i < hiveColumnNames.size() && i < hiveColumnTypes.size(); i++) {
      String columnName = hiveColumnNames.get(i);
      if (hiveColumn.equals(columnName)) {
        return hiveColumnTypes.get(i);
      }
    }

    throw new NoSuchElementException("Could not find Hive column type for " + hiveColumn);
  }

  /**
   * Extracts the table property to allow a custom ColumnVisibility label to be set on updates to be
   * written to an Accumulo table. The value in the table property must be a properly formatted
   * {@link ColumnVisibility}. If not value is present in the table properties, an empty
   * ColumnVisibility is returned.
   *
   * @return The ColumnVisibility to be applied to all updates sent to Accumulo
   */
  public ColumnVisibility getTableVisibilityLabel() {
    String visibilityLabel = tableProperties.getProperty(VISIBILITY_LABEL_KEY, null);
    if (null == visibilityLabel || visibilityLabel.isEmpty()) {
      return DEFAULT_VISIBILITY_LABEL;
    }

    return new ColumnVisibility(visibilityLabel);
  }

  /**
   * Extracts the table property to allow dynamic Accumulo Authorizations to be used when reading
   * data from an Accumulo table. If no Authorizations are provided in the table properties, null is
   * returned to preserve the functionality to read all data that the current user has access to.
   *
   * @return The Authorizations that should be used to read data from Accumulo, null if no
   *         configuration is supplied.
   */
  public Authorizations getAuthorizations() {
    String authorizationStr = tableProperties.getProperty(AUTHORIZATIONS_KEY, null);

    return getAuthorizationsFromValue(authorizationStr);
  }

  /**
   * Create an Authorizations object when the provided value is not null. Will return null,
   * otherwise.
   *
   * @param authorizationStr
   *          Configuration value to parse
   * @return Authorization object or null
   */
  protected static Authorizations getAuthorizationsFromValue(String authorizationStr) {
    if (null == authorizationStr) {
      return null;
    }

    return new Authorizations(authorizationStr);
  }

  /**
   * Extract any configuration on Authorizations to be used from the provided Configuration. If a
   * non-null value is not present in the configuration, a null object is returned
   *
   * @return Authorization built from configuration value, null if no value is present in conf
   */
  public static Authorizations getAuthorizationsFromConf(Configuration conf) {
    Preconditions.checkNotNull(conf);

    String authorizationStr = conf.get(AUTHORIZATIONS_KEY, null);

    return getAuthorizationsFromValue(authorizationStr);
  }
}
