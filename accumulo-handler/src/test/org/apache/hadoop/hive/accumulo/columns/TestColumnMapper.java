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
package org.apache.hadoop.hive.accumulo.columns;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;
import org.apache.hadoop.hive.accumulo.serde.TooManyAccumuloColumnsException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;

/**
 *
 */
public class TestColumnMapper {

  @Test
  public void testNormalMapping() throws TooManyAccumuloColumnsException {
    List<String> rawMappings = Arrays.asList(AccumuloHiveConstants.ROWID, "cf:cq", "cf:_",
        "cf:qual");
    List<String> columnNames = Arrays.asList("row", "col1", "col2", "col3");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);
    ColumnMapper mapper = new ColumnMapper(
        Joiner.on(AccumuloHiveConstants.COMMA).join(rawMappings), ColumnEncoding.STRING.getName(),
        columnNames, columnTypes);

    List<ColumnMapping> mappings = mapper.getColumnMappings();

    Assert.assertEquals(rawMappings.size(), mappings.size());
    Assert.assertEquals(mappings.size(), mapper.size());

    // Compare the Mapper get at offset method to the list of mappings
    Iterator<String> rawIter = rawMappings.iterator();
    Iterator<ColumnMapping> iter = mappings.iterator();
    for (int i = 0; i < mappings.size() && iter.hasNext(); i++) {
      String rawMapping = rawIter.next();
      ColumnMapping mapping = iter.next();
      ColumnMapping mappingByOffset = mapper.get(i);

      Assert.assertEquals(mapping, mappingByOffset);

      // Ensure that we get the right concrete ColumnMapping
      if (AccumuloHiveConstants.ROWID.equals(rawMapping)) {
        Assert.assertEquals(HiveAccumuloRowIdColumnMapping.class, mapping.getClass());
      } else {
        Assert.assertEquals(HiveAccumuloColumnMapping.class, mapping.getClass());
      }
    }

    Assert.assertEquals(0, mapper.getRowIdOffset());
    Assert.assertTrue(mapper.hasRowIdMapping());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultipleRowIDsFails() throws TooManyAccumuloColumnsException {
    new ColumnMapper(AccumuloHiveConstants.ROWID + AccumuloHiveConstants.COMMA
        + AccumuloHiveConstants.ROWID, null, Arrays.asList("row", "row2"),
        Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo));
  }

  @Test
  public void testGetMappingFromHiveColumn() throws TooManyAccumuloColumnsException {
    List<String> hiveColumns = Arrays.asList("rowid", "col1", "col2", "col3");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);
    List<String> rawMappings = Arrays.asList(AccumuloHiveConstants.ROWID, "cf:cq", "cf:_",
        "cf:qual");
    ColumnMapper mapper = new ColumnMapper(
        Joiner.on(AccumuloHiveConstants.COMMA).join(rawMappings), null, hiveColumns, columnTypes);

    for (int i = 0; i < hiveColumns.size(); i++) {
      String hiveColumn = hiveColumns.get(i), accumuloMapping = rawMappings.get(i);
      ColumnMapping mapping = mapper.getColumnMappingForHiveColumn(hiveColumns, hiveColumn);

      Assert.assertEquals(accumuloMapping, mapping.getMappingSpec());
    }
  }

  @Test
  public void testGetTypesString() throws TooManyAccumuloColumnsException {
    List<String> hiveColumns = Arrays.asList("rowid", "col1", "col2", "col3");
    List<String> rawMappings = Arrays.asList(AccumuloHiveConstants.ROWID, "cf:cq", "cf:_",
        "cf:qual");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo);
    ColumnMapper mapper = new ColumnMapper(
        Joiner.on(AccumuloHiveConstants.COMMA).join(rawMappings), null, hiveColumns, columnTypes);

    String typeString = mapper.getTypesString();
    String[] types = StringUtils.split(typeString, AccumuloHiveConstants.COLON);
    Assert.assertEquals(rawMappings.size(), types.length);
    for (String type : types) {
      Assert.assertEquals(serdeConstants.STRING_TYPE_NAME, type);
    }
  }

  @Test
  public void testDefaultBinary() throws TooManyAccumuloColumnsException {
    List<String> hiveColumns = Arrays.asList("rowid", "col1", "col2", "col3", "col4");
    List<String> rawMappings = Arrays.asList(AccumuloHiveConstants.ROWID, "cf:cq", "cf:_#s",
        "cf:qual#s", "cf:qual2");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
    ColumnMapper mapper = new ColumnMapper(
        Joiner.on(AccumuloHiveConstants.COMMA).join(rawMappings), ColumnEncoding.BINARY.getName(),
        hiveColumns, columnTypes);

    List<ColumnMapping> mappings = mapper.getColumnMappings();
    Assert.assertEquals(5, mappings.size());

    Assert.assertEquals(ColumnEncoding.BINARY, mappings.get(0).getEncoding());
    Assert.assertEquals(columnTypes.get(0).toString(), mappings.get(0).getColumnType());

    Assert.assertEquals(ColumnEncoding.BINARY, mappings.get(1).getEncoding());
    Assert.assertEquals(columnTypes.get(1).toString(), mappings.get(1).getColumnType());

    Assert.assertEquals(ColumnEncoding.STRING, mappings.get(2).getEncoding());
    Assert.assertEquals(columnTypes.get(2).toString(), mappings.get(2).getColumnType());

    Assert.assertEquals(ColumnEncoding.STRING, mappings.get(3).getEncoding());
    Assert.assertEquals(columnTypes.get(3).toString(), mappings.get(3).getColumnType());

    Assert.assertEquals(ColumnEncoding.BINARY, mappings.get(4).getEncoding());
    Assert.assertEquals(columnTypes.get(4).toString(), mappings.get(4).getColumnType());

  }

  @Test
  public void testMap() throws TooManyAccumuloColumnsException {
    List<String> hiveColumns = Arrays.asList("rowid", "col1", "col2", "col3");
    List<TypeInfo> columnTypes = Arrays.<TypeInfo> asList(TypeInfoFactory.stringTypeInfo,
        TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.stringTypeInfo,
            TypeInfoFactory.stringTypeInfo), TypeInfoFactory.getMapTypeInfo(
            TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo),
        TypeInfoFactory.stringTypeInfo);
    List<String> rawMappings = Arrays.asList(AccumuloHiveConstants.ROWID, "cf1:*", "cf2:2*",
        "cq3:bar\\*");
    ColumnMapper mapper = new ColumnMapper(
        Joiner.on(AccumuloHiveConstants.COMMA).join(rawMappings), ColumnEncoding.BINARY.getName(),
        hiveColumns, columnTypes);

    List<ColumnMapping> mappings = mapper.getColumnMappings();
    Assert.assertEquals(4, mappings.size());

    Assert.assertEquals(HiveAccumuloRowIdColumnMapping.class, mappings.get(0).getClass());
    Assert.assertEquals(HiveAccumuloMapColumnMapping.class, mappings.get(1).getClass());
    Assert.assertEquals(HiveAccumuloMapColumnMapping.class, mappings.get(2).getClass());
    Assert.assertEquals(HiveAccumuloColumnMapping.class, mappings.get(3).getClass());

    HiveAccumuloRowIdColumnMapping row = (HiveAccumuloRowIdColumnMapping) mappings.get(0);
    Assert.assertEquals(ColumnEncoding.BINARY, row.getEncoding());
    Assert.assertEquals(hiveColumns.get(0), row.getColumnName());
    Assert.assertEquals(columnTypes.get(0).toString(), row.getColumnType());

    HiveAccumuloMapColumnMapping map = (HiveAccumuloMapColumnMapping) mappings.get(1);
    Assert.assertEquals("cf1", map.getColumnFamily());
    Assert.assertEquals("", map.getColumnQualifierPrefix());
    Assert.assertEquals(ColumnEncoding.BINARY, map.getEncoding());
    Assert.assertEquals(hiveColumns.get(1), map.getColumnName());
    Assert.assertEquals(columnTypes.get(1).toString(), map.getColumnType());

    map = (HiveAccumuloMapColumnMapping) mappings.get(2);
    Assert.assertEquals("cf2", map.getColumnFamily());
    Assert.assertEquals("2", map.getColumnQualifierPrefix());
    Assert.assertEquals(ColumnEncoding.BINARY, map.getEncoding());
    Assert.assertEquals(hiveColumns.get(2), map.getColumnName());
    Assert.assertEquals(columnTypes.get(2).toString(), map.getColumnType());

    HiveAccumuloColumnMapping column = (HiveAccumuloColumnMapping) mappings.get(3);
    Assert.assertEquals("cq3", column.getColumnFamily());
    Assert.assertEquals("bar*", column.getColumnQualifier());
    Assert.assertEquals(ColumnEncoding.BINARY, column.getEncoding());
    Assert.assertEquals(hiveColumns.get(3), column.getColumnName());
    Assert.assertEquals(columnTypes.get(3).toString(), column.getColumnType());
  }
}
