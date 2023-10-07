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

package org.apache.hadoop.hive.metastore;

import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionFilterMode;
import org.apache.hadoop.hive.metastore.api.PartitionWithoutSD;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.TestListPartitions;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static junit.framework.TestCase.assertTrue;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test class for list partitions with configurable include/exclude pattern on parameters.
 * Embedded Metastore uses JDO, Remote Metastore uses direct SQL to query the partitions:
 * MetaStoreFactoryForTests.getMetaStores()
 */
@RunWith(Parameterized.class)
@Category(MetastoreCheckinTest.class)
public class TestListPartitionsWithXIncludeParams
    extends TestListPartitions {

  private Configuration hiveConf;
  private Set<String> includeKeys = new HashSet<>();
  private Set<String> excludeKeys = new HashSet<>();
  private Map<String, String> partParams = new HashMap<>();

  public static class PartitionExpressionForMetastoreTest extends PartitionExpressionForMetastore {
    // MetaStoreTestUtils.setConfForStandloneMode will change the default PartitionExpressionForMetastore
    // to DefaultPartitionExpressionProxy, which doesn't support deserializing the Hive filter from a byte array.
    // As this test sits inside the hive-exec module, it's safe to set the hive.metastore.expression.proxy to
    // PartitionExpressionForMetastoreTest.
  }

  public TestListPartitionsWithXIncludeParams(String name,
      AbstractMetaStoreService metaStore) {
    super(name, metaStore);
    partParams.put("key1", "value1");
    partParams.put("akey1", "avalue1");
    partParams.put("akey10", "avalue10");
    partParams.put("excludekey1", "value1");
    partParams.put("excludekey2", "value1");
    includeKeys.add("key1");
    includeKeys.add("akey1");
    excludeKeys.add("excludekey1");
    excludeKeys.add("excludekey2");
    hiveConf = metaStore.getConf();
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.EXPRESSION_PROXY_CLASS,
        PartitionExpressionForMetastoreTest.class.getName());
    MetastoreConf.setVar(hiveConf,
        MetastoreConf.ConfVars.METASTORE_PARTITIONS_PARAMETERS_INCLUDE_PATTERN, "%k_y_");
    MetastoreConf.setVar(hiveConf,
        MetastoreConf.ConfVars.METASTORE_PARTITIONS_PARAMETERS_EXCLUDE_PATTERN, "%exclu%");
  }

  @Override
  protected void addPartition(IMetaStoreClient client, Table table,
      List<String> values) throws TException {
    PartitionBuilder partitionBuilder = new PartitionBuilder().inTable(table);
    values.forEach(val -> partitionBuilder.addValue(val));
    partParams.forEach((k, v) -> partitionBuilder.addPartParam(k, v));
    client.add_partition(partitionBuilder.build(getMetaStore().getConf()));
  }

  @Override
  protected void addPartitions(IMetaStoreClient client, List<Partition> partitions)
      throws TException {
    partitions.stream().forEach(partition -> partition.setParameters(partParams));
    super.addPartitions(client, partitions);
  }

  @Override
  protected void assertPartitionsHaveCorrectParams(List<Partition> partitions) {
    for (int i = 0; i < partitions.size(); i++) {
      Map<String, String> parameters = partitions.get(i).getParameters();
      assertTrue("included parameter key is not found in the partition",
          parameters.keySet().containsAll(includeKeys));
      assertFalse("excluded parameter key is found in the partition",
          parameters.keySet().stream().anyMatch(key -> excludeKeys.contains(key)));
      assertEquals(includeKeys.size(), parameters.size());
    }
  }

  @Test
  public void testGetPartitionsByNames() throws Exception {
    Table t = createTable4PartColsParts(getClient()).table;
    List<String> part_names = Arrays.asList("yyyy=1999/mm=01/dd=02",
        "yyyy=2009/mm=02/dd=10", "yyyy=1999/mm=03/dd=02");
    GetPartitionsByNamesRequest request = convertToGetPartitionsByNamesRequest(
        MetaStoreUtils.prependCatalogToDbName(t.getCatName(), t.getDbName(), hiveConf), t.getTableName(),
        part_names);
    List<Partition> partitions = getClient().getPartitionsByNames(request).getPartitions();
    List<List<String>> values = partitions.stream().map(partition -> partition.getValues()).collect(Collectors.toList());
    assertCorrectPartitionNames(part_names.subList(0, 2), values, Lists.newArrayList("yyyy", "mm", "dd"));
    assertPartitionsHaveCorrectParams(partitions);

    // empty
    part_names = Arrays.asList("yyyy=1999/mm=03/dd=02", "yyyy=2017/mm=02/dd=13");
    request = convertToGetPartitionsByNamesRequest(
        MetaStoreUtils.prependCatalogToDbName(t.getCatName(), t.getDbName(), hiveConf), t.getTableName(),
        part_names);
    partitions = getClient().getPartitionsByNames(request).getPartitions();
    assertTrue(partitions.isEmpty());
  }

  @Test
  public void testGetPartitionsRequest() throws Exception {
    ReturnTable returnTable = createTable4PartColsParts(getClient());
    Table t = returnTable.table;
    GetPartitionsRequest request = new GetPartitionsRequest(t.getDbName(), t.getTableName(),
        new GetProjectionsSpec(), new GetPartitionsFilterSpec());
    request.setCatName(t.getCatName());

    List<Partition> partitions = MetaStoreServerUtils.getPartitionsByProjectSpec(getClient(), request);
    assertPartitionsHaveCorrectParams(partitions);
    List<List<String>> values = partitions.stream().map(partition -> partition.getValues()).collect(Collectors.toList());
    assertEquals(returnTable.testValues, values);

    request.getProjectionSpec()
        .setFieldList(Arrays.asList("dbName", "tableName", "catName", "parameters", "values"));
    partitions = MetaStoreServerUtils.getPartitionsByProjectSpec(getClient(), request);
    assertPartitionsHaveCorrectParams(partitions);
    values = partitions.stream().map(partition -> partition.getValues()).collect(Collectors.toList());
    assertEquals(returnTable.testValues, values);

    request.getFilterSpec().setFilterMode(PartitionFilterMode.BY_VALUES);
    request.getFilterSpec().setFilters(Arrays.asList("2017"));
    partitions = MetaStoreServerUtils.getPartitionsByProjectSpec(getClient(), request);
    assertPartitionsHaveCorrectParams(partitions);
    values = partitions.stream().map(partition -> partition.getValues()).collect(Collectors.toList());
    assertEquals("Two partitions expected", 2, values.size());
    assertEquals(Arrays.asList(Arrays.asList("2017", "10", "26"),
        Arrays.asList("2017", "11", "27")), returnTable.testValues.subList(2, 4));
  }

  @Test
  public void testListPartitionsByExr() throws Exception {
    createTable4PartColsParts(getClient());
    TestMetastoreExpr.ExprBuilder e = new TestMetastoreExpr.ExprBuilder(TABLE_NAME);
    checkExpr(2, e.strCol("yyyy").val("2017").pred("=", 2).build());
    checkExpr(3, e.strCol("mm").val("11").pred(">", 2).build());
    checkExpr(4, e.strCol("dd").val("29").pred(">=", 2).build());
    checkExpr(2, e.strCol("yyyy").val("2017").pred("!=", 2).build());
    checkExpr(1, e.strCol("yyyy").val("2017").pred("=", 2).strCol("mm").val("10")
        .pred(">=", 2).pred("and", 2).build());
    checkExpr(3, e.strCol("dd").val("10").pred("<", 2).strCol("yyyy").val("2009")
        .pred("!=", 2).pred("or", 2).build());
    checkExpr(0, e.strCol("yyyy").val("2019").pred("=", 2).build());
  }

  private void checkExpr(int numParts, ExprNodeGenericFuncDesc expr) throws Exception {
    List<Partition> partitions = new ArrayList<>();
    byte[] exprBytes = SerializationUtilities.serializeObjectWithTypeInformation(expr);
    getClient().listPartitionsByExpr(DB_NAME, TABLE_NAME, exprBytes,
        null, (short) -1, partitions);
    assertEquals("Partition check failed: " + expr.getExprString(), numParts, partitions.size());
    assertPartitionsHaveCorrectParams(partitions);

    PartitionsByExprRequest req = new PartitionsByExprRequest(DB_NAME, TABLE_NAME,
        ByteBuffer.wrap(exprBytes));
    List<org.apache.hadoop.hive.metastore.api.PartitionSpec> msParts =
        new ArrayList<>();
    getClient().listPartitionsSpecByExpr(req, msParts);

    int numPartitions = 0;
    for (org.apache.hadoop.hive.metastore.api.PartitionSpec partitionSpec : msParts) {
      assertTrue(partitionSpec.getPartitionList() == null ||
          partitionSpec.getPartitionList().getPartitions() == null ||
          partitionSpec.getPartitionList().getPartitions().isEmpty());
      for (PartitionWithoutSD partitionWithoutSD: partitionSpec.getSharedSDPartitionSpec().getPartitions()) {
        numPartitions ++;
        Map<String, String> parameters = partitionWithoutSD.getParameters();
        assertTrue("included parameter key is not found in the partition",
            parameters.keySet().containsAll(includeKeys));
        assertFalse("excluded parameter key is found in the partition",
            parameters.keySet().stream().anyMatch(key -> excludeKeys.contains(key)));
        assertEquals(includeKeys.size(), parameters.size());
      }
    }
    assertEquals("Partition check failed: " + expr.getExprString(), numParts, numPartitions);
  }

}
