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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.accumulo.AccumuloHiveRow;
import org.apache.hadoop.hive.accumulo.LazyAccumuloRow;
import org.apache.hadoop.hive.accumulo.columns.ColumnMapping;
import org.apache.hadoop.hive.accumulo.columns.HiveAccumuloRowIdColumnMapping;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazyFactory;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deserialization from Accumulo to LazyAccumuloRow for Hive.
 *
 */
public class AccumuloSerDe extends AbstractSerDe {

  private AccumuloSerDeParameters accumuloSerDeParameters;
  private LazyAccumuloRow cachedRow;
  private ObjectInspector cachedObjectInspector;
  private AccumuloRowSerializer serializer;

  private static final Logger log = LoggerFactory.getLogger(AccumuloSerDe.class);

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    accumuloSerDeParameters = new AccumuloSerDeParameters(configuration, properties, getClass().getName());

    final LazySerDeParameters serDeParams = accumuloSerDeParameters.getSerDeParameters();
    final List<ColumnMapping> mappings = accumuloSerDeParameters.getColumnMappings();
    final List<TypeInfo> columnTypes = accumuloSerDeParameters.getHiveColumnTypes();
    final AccumuloRowIdFactory factory = accumuloSerDeParameters.getRowIdFactory();

    ArrayList<ObjectInspector> columnObjectInspectors = getColumnObjectInspectors(columnTypes, serDeParams, mappings, factory);

    cachedObjectInspector = LazyObjectInspectorFactory.getLazySimpleStructObjectInspector(
        serDeParams.getColumnNames(), columnObjectInspectors, serDeParams.getSeparators()[0],
        serDeParams.getNullSequence(), serDeParams.isLastColumnTakesRest(),
        serDeParams.isEscaped(), serDeParams.getEscapeChar());

    cachedRow = new LazyAccumuloRow((LazySimpleStructObjectInspector) cachedObjectInspector);

    serializer = new AccumuloRowSerializer(accumuloSerDeParameters.getRowIdOffset(),
        accumuloSerDeParameters.getSerDeParameters(), accumuloSerDeParameters.getColumnMappings(),
        accumuloSerDeParameters.getTableVisibilityLabel(),
        accumuloSerDeParameters.getRowIdFactory());

    if (log.isInfoEnabled()) {
      log.info("Initialized with {} type: {}", accumuloSerDeParameters.getSerDeParameters()
          .getColumnNames(), accumuloSerDeParameters.getSerDeParameters().getColumnTypes());
    }
  }
  
  @Override
  protected List<String> parseColumnNames() {
    // Are calculated in AccumuloSerDeParameters
    return Collections.emptyList();
  }
  
  @Override
  protected List<TypeInfo> parseColumnTypes() {
    // Are calculated in AccumuloSerDeParameters
    return Collections.emptyList();
  }

  protected ArrayList<ObjectInspector> getColumnObjectInspectors(List<TypeInfo> columnTypes,
      LazySerDeParameters serDeParams, List<ColumnMapping> mappings, AccumuloRowIdFactory factory)
      throws SerDeException {
    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        columnTypes.size());
    for (int i = 0; i < columnTypes.size(); i++) {
      TypeInfo type = columnTypes.get(i);
      ColumnMapping mapping = mappings.get(i);
      if (mapping instanceof HiveAccumuloRowIdColumnMapping) {
        columnObjectInspectors.add(factory.createRowIdObjectInspector(type));
      } else {
        columnObjectInspectors.add(LazyFactory.createLazyObjectInspector(type,
            serDeParams.getSeparators(), 1, serDeParams.getNullSequence(), serDeParams.isEscaped(),
            serDeParams.getEscapeChar()));
      }
    }

    return columnObjectInspectors;
  }

  /***
   * For testing purposes.
   */
  public LazyAccumuloRow getCachedRow() {
    return cachedRow;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Mutation.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    try {
      return serializer.serialize(o, objectInspector);
    } catch (IOException e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    if (!(writable instanceof AccumuloHiveRow)) {
      throw new SerDeException(getClass().getName() + " : " + "Expected AccumuloHiveRow. Got "
          + writable.getClass().getName());
    }

    cachedRow.init((AccumuloHiveRow) writable, accumuloSerDeParameters.getColumnMappings(),
        accumuloSerDeParameters.getRowIdFactory());

    return cachedRow;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  public AccumuloSerDeParameters getParams() {
    return accumuloSerDeParameters;
  }

  public boolean getIteratorPushdown() {
    return accumuloSerDeParameters.getIteratorPushdown();
  }

  protected AccumuloRowSerializer getSerializer() {
    return serializer;
  }
}
