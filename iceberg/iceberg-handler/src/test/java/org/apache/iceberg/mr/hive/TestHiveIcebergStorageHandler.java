/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.collections4.IterableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveIcebergStorageHandler {

  private static final ExprNodeDesc DATA_COL = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "a", "t", false);
  private static final ExprNodeDesc SPEC_ID_COL =
      new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, VirtualColumn.PARTITION_SPEC_ID.getName(), "t", true);
  private static final ExprNodeDesc FILE_PATH_COL =
      new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, VirtualColumn.FILE_PATH.getName(), "t", true);

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Mock
  private Snapshot anySnapshot;
  @Mock
  private Snapshot appendSnapshot;
  @Mock
  private Snapshot deleteSnapshot;

  @Before
  public void before() {
    when(anySnapshot.snapshotId()).thenReturn(42L);

    Mockito.lenient().when(appendSnapshot.snapshotId()).thenReturn(20L);
    Map<String, String> summary = Maps.newHashMap();
    summary.put(SnapshotSummary.ADDED_RECORDS_PROP, "12");
    Mockito.lenient().when(appendSnapshot.summary()).thenReturn(summary);
    when(appendSnapshot.operation()).thenReturn("append");

    Mockito.lenient().when(deleteSnapshot.snapshotId()).thenReturn(100L);
    when(deleteSnapshot.operation()).thenReturn("delete");
    summary = Maps.newHashMap();
    summary.put(SnapshotSummary.DELETED_RECORDS_PROP, "3");
    Mockito.lenient().when(deleteSnapshot.summary()).thenReturn(summary);
  }

  /**
   * Catalog secrets follow the same decision as the server-side planning marker: a table Iceberg did
   * not resolve to a server-planning {@code RESTTable} gets no {@code iceberg.catalog.<catalog>.*}
   * secrets, even with the Hive kill switch on. The positive case needs a REST server and is in
   * {@code TestHiveIcebergServerSideScanPlanningServerIT}.
   */
  @Test
  public void configureInputJobCredentialsSkipsCatalogSecretsForTableNotPlannedServerSide() throws IOException {
    String location = temp.newFolder().toURI().toString();
    Configuration conf = new Configuration();
    new HadoopTables(conf).create(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())), location);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_REST_SERVER_SIDE_SCAN_PLANNING_ENABLED, true);
    conf.set(IcebergCatalogProperties.catalogPropertyConfigKey(Catalogs.ICEBERG_HADOOP_TABLE_NAME, "credential"),
        "catalog-credential");

    Properties props = new Properties();
    props.setProperty(InputFormatConfig.CATALOG_NAME, Catalogs.ICEBERG_HADOOP_TABLE_NAME);
    props.setProperty(Catalogs.LOCATION, location);
    props.setProperty(Catalogs.NAME, "default.tbl");
    TableDesc tableDesc = new TableDesc();
    tableDesc.setProperties(props);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    storageHandler.setConf(conf);
    Map<String, String> secrets = Maps.newHashMap();
    storageHandler.configureInputJobCredentials(tableDesc, secrets);

    assertThat(secrets.isEmpty(), is(true));
  }

  @Test
  public void testHasAppendsOnlyReturnsNullWhenTableIsEmpty() {
    SnapshotContext since = new SnapshotContext(42);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(Collections.emptyList(), since);

    assertThat(result, is(nullValue()));
  }

  @Test
  public void testHasAppendsOnlyReturnsNullWhenTableIsEmptyAndGivenSnapShotIsNull() {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(Collections.emptyList(), null);

    assertThat(result, is(true));
  }

  @Test
  public void testHasAppendsOnlyTrueWhenGivenSnapShotIsNull() {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(singletonList(appendSnapshot), null);

    assertThat(result, is(true));
  }

  @Test
  public void testHasAppendsOnlyFalseWhenGivenSnapShotIsNullButHasNonAppend() {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(asList(appendSnapshot, deleteSnapshot), null);

    assertThat(result, is(false));
  }

  @Test
  public void testHasAppendsOnlyTrueWhenOnlyAppendsAfterGivenSnapshot() {
    SnapshotContext since = new SnapshotContext(42);
    List<Snapshot> snapshotList = asList(anySnapshot, appendSnapshot);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(snapshotList, since);

    assertThat(result, is(true));
  }

  @Test
  public void testHasAppendsOnlyFalseWhenNotOnlyAppendsAfterGivenSnapshot() {
    SnapshotContext since = new SnapshotContext(42);
    List<Snapshot> snapshotList = Arrays.asList(anySnapshot, appendSnapshot, deleteSnapshot);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(snapshotList, since);

    assertThat(result, is(false));
  }

  @Test
  public void testHasAppendsOnlyReturnsNullWhenGivenSnapshotNotInTheList() {
    SnapshotContext since = new SnapshotContext(1);
    List<Snapshot> snapshotList = Arrays.asList(anySnapshot, appendSnapshot, deleteSnapshot);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Boolean result = storageHandler.hasAppendsOnly(snapshotList, since);

    assertThat(result, is(nullValue()));
  }

  @Test
  public void testGetSnapshotContextsReturnsEmptyIterableWhenTableIsEmpty() {
    SnapshotContext since = new SnapshotContext(42);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Iterable<SnapshotContext> result = storageHandler.getSnapshots(Collections.emptyList(), since);

    assertThat(result.iterator().hasNext(), is(false));
  }

  @Test
  public void testGetSnapshotContextsReturnsEmptyIterableWhenTableIsEmptyAndGivenSnapShotIsNull() {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Iterable<SnapshotContext> result = storageHandler.getSnapshots(Collections.emptyList(), null);

    assertThat(result.iterator().hasNext(), is(false));
  }

  @Test
  public void testGetSnapshotContextsReturnsAllSnapshotsWhenGivenSnapshotIsNull() {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Iterable<SnapshotContext> result = storageHandler.getSnapshots(asList(appendSnapshot, deleteSnapshot), null);

    List<SnapshotContext> resultList = IterableUtils.toList(result);
    assertThat(resultList.size(), is(2));
    assertThat(resultList.get(0).getSnapshotId(), is(appendSnapshot.snapshotId()));
    assertThat(resultList.get(0).getOperation(), is(SnapshotContext.WriteOperationType.APPEND));
    assertThat(resultList.get(0).getAddedRowCount(), is(12L));
    assertThat(resultList.get(0).getDeletedRowCount(), is(0L));

    assertThat(resultList.get(1).getSnapshotId(), is(deleteSnapshot.snapshotId()));
    assertThat(resultList.get(1).getOperation(), is(SnapshotContext.WriteOperationType.DELETE));
    assertThat(resultList.get(1).getAddedRowCount(), is(0L));
    assertThat(resultList.get(1).getDeletedRowCount(), is(3L));
  }

  @Test
  public void testGetSnapshotContextsReturnsEmptyIterableWhenGivenSnapshotNotInTheList() {
    SnapshotContext since = new SnapshotContext(1);
    List<Snapshot> snapshotList = Arrays.asList(anySnapshot, appendSnapshot, deleteSnapshot);

    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    Iterable<SnapshotContext> result = storageHandler.getSnapshots(snapshotList, since);

    assertThat(result.iterator().hasNext(), is(false));
  }

  @Test
  public void testDecomposePredicateLeavesOutConjunctsReadingVirtualColumns() throws UDFArgumentException {
    ExprNodeGenericFuncDesc dataFilter = func(new GenericUDFOPEqual(), DATA_COL, constant(1));
    ExprNodeGenericFuncDesc filter = func(new GenericUDFOPAnd(), dataFilter, func(new GenericUDFOPEqual(), SPEC_ID_COL,
        constant(0)), func(new GenericUDFOPNotNull(), FILE_PATH_COL));

    DecomposedPredicate decomposed = decompose(filter);
    assertThat(decomposed.pushedPredicate.isSame(dataFilter), is(true));
    assertThat(decomposed.residualPredicate, sameInstance(filter));

    filter = func(new GenericUDFOPAnd(), func(new GenericUDFOPEqual(), SPEC_ID_COL, constant(0)),
        func(new GenericUDFOPNotNull(), FILE_PATH_COL));
    assertThat(decompose(filter).pushedPredicate, is(nullValue()));
  }

  @Test
  public void testDecomposePredicatePushesNothingWhenOtherOperatorReadsVirtualColumn() throws UDFArgumentException {
    List<ExprNodeGenericFuncDesc> filters = asList(
        func(new GenericUDFOPOr(), func(new GenericUDFOPEqual(), DATA_COL, constant(1)),
            func(new GenericUDFOPEqual(), SPEC_ID_COL, constant(0))),
        func(new GenericUDFBetween(), constant(false), SPEC_ID_COL, constant(0), constant(1)),
        func(new GenericUDFIn(), SPEC_ID_COL, constant(0), constant(1)),
        func(new GenericUDFOPEqual(), SPEC_ID_COL, constant(0)));

    for (ExprNodeGenericFuncDesc filter : filters) {
      DecomposedPredicate decomposed = decompose(filter);
      assertThat(filter.getExprString(), decomposed.pushedPredicate, is(nullValue()));
      assertThat(decomposed.residualPredicate, sameInstance(filter));
    }
  }

  @Test
  public void testDecomposePredicatePushesEntireFilterWithoutVirtualColumns() throws UDFArgumentException {
    ExprNodeGenericFuncDesc filter = func(new GenericUDFOPOr(), func(new GenericUDFOPEqual(), DATA_COL, constant(1)),
        func(new GenericUDFOPEqual(), DATA_COL, constant(2)));

    DecomposedPredicate decomposed = decompose(filter);
    assertThat(decomposed.pushedPredicate.isSame(filter), is(true));
    assertThat(decomposed.pushedPredicate, not(sameInstance(filter)));
    assertThat(decomposed.residualPredicate, sameInstance(filter));
  }

  private static DecomposedPredicate decompose(ExprNodeGenericFuncDesc filter) {
    HiveIcebergStorageHandler storageHandler = new HiveIcebergStorageHandler();
    storageHandler.setConf(new Configuration());
    return storageHandler.decomposePredicate(new JobConf(), null, filter);
  }

  private static ExprNodeGenericFuncDesc func(GenericUDF udf, ExprNodeDesc... children) throws UDFArgumentException {
    return ExprNodeGenericFuncDesc.newInstance(udf, asList(children));
  }

  private static ExprNodeConstantDesc constant(Object value) {
    return new ExprNodeConstantDesc(value);
  }
}
