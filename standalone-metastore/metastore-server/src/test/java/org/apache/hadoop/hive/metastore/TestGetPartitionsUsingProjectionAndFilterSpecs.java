/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionFilterMode;
import org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpecWithSharedSD;
import org.apache.hadoop.hive.metastore.api.PartitionWithoutSD;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.hadoop.hive.metastore.ColumnType.SERIALIZATION_FORMAT;

/**
 * Tests for getPartitionsWithSpecs metastore API. This test create some partitions and makes sure
 * that getPartitionsWithSpecs returns results which are comparable with the get_partitions API when
 * various combinations of projection spec are set. Also checks the JDO code path in addition to
 * directSQL code path
 */
@Category(MetastoreCheckinTest.class)
public class TestGetPartitionsUsingProjectionAndFilterSpecs {
  private static final Logger LOG = LoggerFactory.getLogger(TestGetPartitionsUsingProjectionAndFilterSpecs.class);
  protected static Configuration conf = MetastoreConf.newMetastoreConf();
  private static int port;
  private static final String dbName = "test_projection_db";
  private static final String tblName = "test_projection_table";
  private List<Partition> origPartitions;
  private Table tbl;
  private static final String EXCLUDE_KEY_PREFIX = "exclude";
  private HiveMetaStoreClient client;

  @BeforeClass
  public static void startMetaStoreServer() throws Exception {
    conf.set("hive.in.test", "true");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setLongVar(conf, ConfVars.BATCH_RETRIEVE_MAX, 2);
    MetastoreConf.setLongVar(conf, ConfVars.LIMIT_PARTITION_REQUEST, 100);
    port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    LOG.info("Starting MetaStore Server on port " + port);

    try (HiveMetaStoreClient client = createClient()) {
      new DatabaseBuilder().setName(dbName).create(client, conf);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try (HiveMetaStoreClient client = createClient()) {
      client.dropDatabase(dbName, true, true, true);
    }
  }

  @Before
  public void setup() throws TException {
    // This is default case with setugi off for both client and server
    client = createClient();
    createTestTables();
    origPartitions = client.listPartitions(dbName, tblName, (short) -1);
    tbl = client.getTable(dbName, tblName);
    // set directSQL to true explicitly
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL.getVarname(), "true");
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL_DDL.getVarname(), "true");
  }

  @After
  public void cleanup() {
    dropTestTables();
    client.close();
    client = null;
  }

  private void dropTestTables() {
    try {
      client.dropTable(dbName, tblName);
    } catch (TException e) {
      // ignored
    }
  }

  private void createTestTables() throws TException {
    if (client.tableExists(dbName, tblName)) {
      LOG.info("Table is already existing. Dropping it and then recreating");
      client.dropTable(dbName, tblName);
    }
    new TableBuilder().setTableName(tblName).setDbName(dbName).setCols(Arrays
        .asList(new FieldSchema("col1", "string", "c1 comment"),
            new FieldSchema("col2", "int", "c2 comment"))).setPartCols(Arrays
        .asList(new FieldSchema("state", "string", "state comment"),
            new FieldSchema("city", "string", "city comment")))
        .setTableParams(new HashMap<String, String>(2) {{
          put("tableparam1", "tableval1");
          put("tableparam2", "tableval2");
        }})
        .setBucketCols(Collections.singletonList("col1"))
        .addSortCol("col2", 1)
        .addSerdeParam(SERIALIZATION_FORMAT, "1").setSerdeName(tblName)
        .setSerdeLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
        .setInputFormat("org.apache.hadoop.hive.ql.io.HiveInputFormat")
        .setOutputFormat("org.apache.hadoop.hive.ql.io.HiveOutputFormat")
        .create(client, conf);

    Table table = client.getTable(dbName, tblName);
    Assert.assertTrue("Table " + dbName + "." + tblName + " does not exist",
        client.tableExists(dbName, tblName));

    List<Partition> partitions = new ArrayList<>();
    partitions.add(createPartition(Arrays.asList("CA", "SanFrancisco"), table));
    partitions.add(createPartition(Arrays.asList("CA", "PaloAlto"), table));
    partitions.add(createPartition(Arrays.asList("WA", "Seattle"), table));
    partitions.add(createPartition(Arrays.asList("AZ", "Phoenix"), table));

    client.add_partitions(partitions);
  }

  private Partition createPartition(List<String> vals, Table table) throws MetaException {
    return new PartitionBuilder()
        .inTable(table)
        .setValues(vals)
        .addPartParam("key1", "S1")
        .addPartParam("key2", "S2")
        .addPartParam(EXCLUDE_KEY_PREFIX + "key1", "e1")
        .addPartParam(EXCLUDE_KEY_PREFIX + "key2", "e2")
        .setBucketCols(table.getSd().getBucketCols())
        .setSortCols(table.getSd().getSortCols())
        .setSerdeName(table.getSd().getSerdeInfo().getName())
        .setSerdeLib(table.getSd().getSerdeInfo().getSerializationLib())
        .setSerdeParams(table.getSd().getSerdeInfo().getParameters())
        .build(conf);
  }

  private static HiveMetaStoreClient createClient() throws MetaException {
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
    return new HiveMetaStoreClient(conf);
  }

  @Test
  public void testGetPartitions() throws TException {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);
    validateBasic(response);
  }

  @Test
  public void testPartitionProjectionEmptySpec() throws Throwable {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();

    projectSpec.setFieldList(new ArrayList<>(0));
    projectSpec.setExcludeParamKeyPattern("exclude%");

    GetPartitionsResponse response;
    response = client.getPartitionsWithSpecs(request);
    Assert.assertEquals(1, response.getPartitionSpec().size());
    PartitionSpec partitionSpec = response.getPartitionSpec().get(0);
    PartitionSpecWithSharedSD partitionSpecWithSharedSD = partitionSpec.getSharedSDPartitionSpec();

    StorageDescriptor sharedSD = partitionSpecWithSharedSD.getSd();
    Assert.assertNotNull(sharedSD);
    // everything except location in sharedSD should be same
    StorageDescriptor origSd = origPartitions.get(0).getSd().deepCopy();
    origSd.unsetLocation();
    StorageDescriptor sharedSDCopy = sharedSD.deepCopy();
    sharedSDCopy.unsetLocation();
    Assert.assertEquals(origSd, sharedSDCopy);

    List<PartitionWithoutSD> partitionWithoutSDS = partitionSpecWithSharedSD.getPartitions();
    Assert.assertNotNull(partitionWithoutSDS);
    Assert.assertEquals("Unexpected number of partitions returned",
        origPartitions.size(), partitionWithoutSDS.size());
    for (int i = 0; i < origPartitions.size(); i++) {
      Partition origPartition = origPartitions.get(i);
      PartitionWithoutSD retPartition = partitionWithoutSDS.get(i);
      Assert.assertEquals(origPartition.getCreateTime(), retPartition.getCreateTime());
      Assert.assertEquals(origPartition.getLastAccessTime(), retPartition.getLastAccessTime());
      Assert.assertEquals(origPartition.getSd().getLocation(),
          sharedSD.getLocation() + retPartition.getRelativePath());
      Assert.assertFalse("excluded parameter key is found in the response",
          retPartition.getParameters().containsKey(EXCLUDE_KEY_PREFIX + "key1"));
      Assert.assertFalse("excluded parameter key is found in the response",
          retPartition.getParameters().containsKey(EXCLUDE_KEY_PREFIX + "key2"));
      Assert.assertEquals("Additional parameters returned",
          3, retPartition.getParameters().size());
    }
  }

  @Test
  public void testPartitionProjectionAllSingleValuedFields() throws Throwable {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();

    List<String> projectedFields = Arrays
        .asList("dbName", "tableName", "createTime", "lastAccessTime", "sd.location",
            "sd.inputFormat", "sd.outputFormat", "sd.compressed", "sd.numBuckets",
            "sd.serdeInfo.name", "sd.serdeInfo.serializationLib"/*, "sd.serdeInfo.serdeType"*/);
    //TODO directSQL does not support serdeType, serializerClass and deserializerClass in serdeInfo
    projectSpec.setFieldList(projectedFields);

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);
    Assert.assertEquals(1, response.getPartitionSpec().size());
    PartitionSpec partitionSpec = response.getPartitionSpec().get(0);
    Assert.assertTrue("DbName is not set", partitionSpec.isSetDbName());
    Assert.assertTrue("tableName is not set", partitionSpec.isSetTableName());
    PartitionSpecWithSharedSD partitionSpecWithSharedSD = partitionSpec.getSharedSDPartitionSpec();

    StorageDescriptor sharedSD = partitionSpecWithSharedSD.getSd();
    Assert.assertNotNull(sharedSD);
    List<PartitionWithoutSD> partitionWithoutSDS = partitionSpecWithSharedSD.getPartitions();
    Assert.assertNotNull(partitionWithoutSDS);
    Assert.assertEquals(partitionWithoutSDS.size(), origPartitions.size());
    comparePartitionForSingleValuedFields(projectedFields, sharedSD, partitionWithoutSDS, 0);
  }

  @Test
  public void testProjectionUsingJDO() throws Throwable {
    // disable direct SQL to make sure
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL.getVarname(), "false");
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    List<String> projectedFields = Collections.singletonList("sd.location");
    projectSpec.setFieldList(projectedFields);

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);
    Assert.assertEquals(1, response.getPartitionSpec().size());
    PartitionSpec partitionSpec = response.getPartitionSpec().get(0);
    Assert.assertTrue("DbName is not set", partitionSpec.isSetDbName());
    Assert.assertTrue("tableName is not set", partitionSpec.isSetTableName());
    PartitionSpecWithSharedSD partitionSpecWithSharedSD = partitionSpec.getSharedSDPartitionSpec();

    StorageDescriptor sharedSD = partitionSpecWithSharedSD.getSd();
    Assert.assertNotNull(sharedSD);
    List<PartitionWithoutSD> partitionWithoutSDS = partitionSpecWithSharedSD.getPartitions();
    Assert.assertNotNull(partitionWithoutSDS);
    Assert.assertEquals(partitionWithoutSDS.size(), origPartitions.size());
    comparePartitionForSingleValuedFields(projectedFields, sharedSD, partitionWithoutSDS, 0);

    // set all the single-valued fields and try using JDO
    request = getGetPartitionsRequest();
    projectSpec = request.getProjectionSpec();
    projectedFields = Arrays
        .asList("dbName", "tableName", "createTime", "lastAccessTime", "sd.location",
            "sd.inputFormat", "sd.outputFormat", "sd.compressed", "sd.numBuckets",
            "sd.serdeInfo.name", "sd.serdeInfo.serializationLib", "sd.serdeInfo.serdeType",
            "sd.serdeInfo.serializerClass", "sd.serdeInfo.deserializerClass");
    projectSpec.setFieldList(projectedFields);

    response = client.getPartitionsWithSpecs(request);
    Assert.assertEquals(1, response.getPartitionSpec().size());
    partitionSpec = response.getPartitionSpec().get(0);
    Assert.assertTrue("DbName is not set", partitionSpec.isSetDbName());
    Assert.assertTrue("tableName is not set", partitionSpec.isSetTableName());
    partitionSpecWithSharedSD = partitionSpec.getSharedSDPartitionSpec();

    sharedSD = partitionSpecWithSharedSD.getSd();
    Assert.assertNotNull(sharedSD);
    partitionWithoutSDS = partitionSpecWithSharedSD.getPartitions();
    Assert.assertNotNull(partitionWithoutSDS);
    Assert.assertEquals(partitionWithoutSDS.size(), origPartitions.size());
    comparePartitionForSingleValuedFields(projectedFields, sharedSD, partitionWithoutSDS, 0);
  }

  /**
   * Confirms if the partitionWithoutSD object at partitionWithoutSDSIndex index has all the
   * projected fields set to values which are same as the ones set in origPartitions
   * @param projectedFields
   * @param sharedSD
   * @param partitionWithoutSDS
   * @param partitionWithoutSDSIndex
   * @throws IllegalAccessException
   * @throws InvocationTargetException
   * @throws NoSuchMethodException
   */
  private void comparePartitionForSingleValuedFields(List<String> projectedFields,
      StorageDescriptor sharedSD, List<PartitionWithoutSD> partitionWithoutSDS, int partitionWithoutSDSIndex)
      throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    for (Partition origPart : origPartitions) {
      for (String projectField : projectedFields) {
        // dbname, tableName and catName is not stored in partition
        if (projectField.equals("dbName") || projectField.equals("tableName") || projectField
            .equals("catName"))
          continue;
        if (projectField.startsWith("sd")) {
          String sdPropertyName = projectField.substring(projectField.indexOf("sd.") + 3);
          if (sdPropertyName.equals("location")) {
            // in case of location sharedSD has the base location and partition has relative location
            Assert.assertEquals("Location does not match", origPart.getSd().getLocation(),
                sharedSD.getLocation() + partitionWithoutSDS.get(partitionWithoutSDSIndex).getRelativePath());
          } else {
            Assert.assertEquals(PropertyUtils.getNestedProperty(origPart, projectField),
                PropertyUtils.getNestedProperty(sharedSD, sdPropertyName));
          }
        } else {
          Assert.assertEquals(PropertyUtils.getNestedProperty(origPart, projectField),
              PropertyUtils.getNestedProperty(partitionWithoutSDS.get(partitionWithoutSDSIndex), projectField));
        }
      }
      partitionWithoutSDSIndex++;
    }
  }

  @Test
  public void testPartitionProjectionAllMultiValuedFields() throws Throwable {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    List<String> projectedFields = Arrays
        .asList("values", "parameters", "sd.cols", "sd.bucketCols", "sd.sortCols", "sd.parameters",
            "sd.skewedInfo", "sd.serdeInfo.parameters");
    projectSpec.setFieldList(projectedFields);

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);

    Assert.assertEquals(1, response.getPartitionSpec().size());
    PartitionSpec partitionSpec = response.getPartitionSpec().get(0);
    PartitionSpecWithSharedSD partitionSpecWithSharedSD = partitionSpec.getSharedSDPartitionSpec();
    Assert.assertEquals(origPartitions.size(), partitionSpecWithSharedSD.getPartitions().size());
    StorageDescriptor sharedSD = partitionSpecWithSharedSD.getSd();
    for (int i = 0; i < origPartitions.size(); i++) {
      Partition origPartition = origPartitions.get(i);
      PartitionWithoutSD retPartition = partitionSpecWithSharedSD.getPartitions().get(i);
      for (String projectedField : projectedFields) {
        switch (projectedField) {
        case "values":
          validateList(origPartition.getValues(), retPartition.getValues());
          break;
        case "parameters":
          validateMap(origPartition.getParameters(), retPartition.getParameters());
          break;
        case "sd.cols":
          validateList(origPartition.getSd().getCols(), sharedSD.getCols());
          break;
        case "sd.bucketCols":
          validateList(origPartition.getSd().getBucketCols(), sharedSD.getBucketCols());
          break;
        case "sd.sortCols":
          validateList(origPartition.getSd().getSortCols(), sharedSD.getSortCols());
          break;
        case "sd.parameters":
          validateMap(origPartition.getSd().getParameters(), sharedSD.getParameters());
          break;
        case "sd.skewedInfo":
          if (!origPartition.getSd().getSkewedInfo().getSkewedColNames().isEmpty()) {
            validateList(origPartition.getSd().getSkewedInfo().getSkewedColNames(),
                sharedSD.getSkewedInfo().getSkewedColNames());
          }
          if (!origPartition.getSd().getSkewedInfo().getSkewedColValues().isEmpty()) {
            for (int i1 = 0;
                 i1 < origPartition.getSd().getSkewedInfo().getSkewedColValuesSize(); i1++) {
              validateList(origPartition.getSd().getSkewedInfo().getSkewedColValues().get(i1),
                  sharedSD.getSkewedInfo().getSkewedColValues().get(i1));
            }
          }
          if (!origPartition.getSd().getSkewedInfo().getSkewedColValueLocationMaps().isEmpty()) {
            validateMap(origPartition.getSd().getSkewedInfo().getSkewedColValueLocationMaps(),
                sharedSD.getSkewedInfo().getSkewedColValueLocationMaps());
          }
          break;
        case "sd.serdeInfo.parameters":
          validateMap(origPartition.getSd().getSerdeInfo().getParameters(),
              sharedSD.getSerdeInfo().getParameters());
          break;
        default:
          throw new IllegalArgumentException("Invalid field " + projectedField);
        }
      }
    }
  }

  @Test
  public void testPartitionProjectionIncludeParameters() throws Throwable {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec
        .setFieldList(Arrays.asList("dbName", "tableName", "catName", "parameters", "values"));
    projectSpec.setIncludeParamKeyPattern(EXCLUDE_KEY_PREFIX + "%");

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);

    PartitionSpecWithSharedSD partitionSpecWithSharedSD =
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
    Assert.assertNotNull("All the partitions should be returned in sharedSD spec",
        partitionSpecWithSharedSD);
    PartitionListComposingSpec partitionListComposingSpec =
        response.getPartitionSpec().get(0).getPartitionList();
    Assert.assertNull("Partition list composing spec should be null since all the "
        + "partitions are expected to be in sharedSD spec", partitionListComposingSpec);
    for (PartitionWithoutSD retPartion : partitionSpecWithSharedSD.getPartitions()) {
      Assert.assertTrue("included parameter key is not found in the response",
          retPartion.getParameters().containsKey(EXCLUDE_KEY_PREFIX + "key1"));
      Assert.assertTrue("included parameter key is not found in the response",
          retPartion.getParameters().containsKey(EXCLUDE_KEY_PREFIX + "key2"));
      Assert.assertEquals("Additional parameters returned other than inclusion keys",
          2, retPartion.getParameters().size());
    }
  }

  @Test
  public void testPartitionProjectionIncludeExcludeParameters() throws Throwable {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec
        .setFieldList(Arrays.asList("dbName", "tableName", "catName", "parameters", "values"));
    // test parameter key inclusion using setIncludeParamKeyPattern
    projectSpec.setIncludeParamKeyPattern(EXCLUDE_KEY_PREFIX + "%");
    projectSpec.setExcludeParamKeyPattern("%key1%");

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);

    PartitionSpecWithSharedSD partitionSpecWithSharedSD =
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
    Assert.assertNotNull("All the partitions should be returned in sharedSD spec",
        partitionSpecWithSharedSD);
    PartitionListComposingSpec partitionListComposingSpec =
        response.getPartitionSpec().get(0).getPartitionList();
    Assert.assertNull("Partition list composing spec should be null since all the "
        + "partitions are expected to be in sharedSD spec", partitionListComposingSpec);
    for (PartitionWithoutSD retPartion : partitionSpecWithSharedSD.getPartitions()) {
      Assert.assertFalse("excluded parameter key is found in the response",
          retPartion.getParameters().containsKey(EXCLUDE_KEY_PREFIX + "key1"));
      Assert.assertTrue("included parameter key is not found in the response",
          retPartion.getParameters().containsKey(EXCLUDE_KEY_PREFIX + "key2"));
      Assert.assertEquals("Additional parameters returned other than inclusion keys",
          1, retPartion.getParameters().size());
    }
  }

  @Test
  public void testPartitionProjectionExcludeParameters() throws Throwable {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec
        .setFieldList(Arrays.asList("dbName", "tableName", "catName", "parameters", "values"));
    projectSpec.setExcludeParamKeyPattern(EXCLUDE_KEY_PREFIX + "%");

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);

    PartitionSpecWithSharedSD partitionSpecWithSharedSD =
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
    Assert.assertNotNull("All the partitions should be returned in sharedSD spec",
        partitionSpecWithSharedSD);
    PartitionListComposingSpec partitionListComposingSpec =
        response.getPartitionSpec().get(0).getPartitionList();
    Assert.assertNull("Partition list composing spec should be null", partitionListComposingSpec);
    for (PartitionWithoutSD retPartion : partitionSpecWithSharedSD.getPartitions()) {
      Assert.assertFalse("excluded parameter key is found in the response",
          retPartion.getParameters().containsKey(EXCLUDE_KEY_PREFIX + "key1"));
      Assert.assertFalse("excluded parameter key is found in the response",
          retPartion.getParameters().containsKey(EXCLUDE_KEY_PREFIX + "key2"));
    }
  }

  @Test
  public void testNestedMultiValuedFieldProjection() throws TException {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec.setFieldList(Arrays.asList("sd.cols.name", "sd.cols.type"));

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);

    PartitionSpecWithSharedSD partitionSpecWithSharedSD =
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
    StorageDescriptor sharedSD = partitionSpecWithSharedSD.getSd();
    Assert.assertNotNull("sd.cols were requested but was not returned", sharedSD.getCols());
    for (FieldSchema col : sharedSD.getCols()) {
      Assert.assertTrue("sd.cols.name was requested but was not returned", col.isSetName());
      Assert.assertTrue("sd.cols.type was requested but was not returned", col.isSetType());
      Assert.assertFalse("sd.cols.comment was not requested but was returned", col.isSetComment());
    }
  }

  @Test
  public void testParameterExpansion() throws TException {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec.setFieldList(Arrays.asList("sd.cols", "sd.serdeInfo"));

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);

    PartitionSpecWithSharedSD partitionSpecWithSharedSD =
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
    StorageDescriptor sharedSD = partitionSpecWithSharedSD.getSd();
    Assert.assertNotNull("sd.cols were requested but was not returned", sharedSD.getCols());
    Assert.assertEquals("Returned serdeInfo does not match with original serdeInfo",
        origPartitions.get(0).getSd().getCols(), sharedSD.getCols());

    Assert
        .assertNotNull("sd.serdeInfo were requested but was not returned", sharedSD.getSerdeInfo());
    Assert.assertEquals("Returned serdeInfo does not match with original serdeInfo",
        origPartitions.get(0).getSd().getSerdeInfo(), sharedSD.getSerdeInfo());
  }

  @Test
  public void testNonStandardPartitions() throws TException {
    String testTblName = "test_non_standard";
    new TableBuilder()
        .setTableName(testTblName)
        .setDbName(dbName)
        .addCol("ns_c1", "string", "comment 1")
        .addCol("ns_c2", "int", "comment 2")
        .addPartCol("part", "string")
        .addPartCol("city", "string")
        .addBucketCol("ns_c1")
        .addSortCol("ns_c2", 1)
        .addTableParam("tblparamKey", "Partitions of this table are not located within table directory")
        .create(client, conf);

    Table table = client.getTable(dbName, testTblName);
    Assert.assertNotNull("Unable to create a test table ", table);

    List<Partition> partitions = new ArrayList<>();
    partitions.add(createPartition(Arrays.asList("p1", "SanFrancisco"), table));
    partitions.add(createPartition(Arrays.asList("p1", "PaloAlto"), table));
    partitions.add(createPartition(Arrays.asList("p2", "Seattle"), table));
    partitions.add(createPartition(Arrays.asList("p2", "Phoenix"), table));

    client.add_partitions(partitions);
    // change locations of two of the partitions outside table directory
    List<Partition> testPartitions = client.listPartitions(dbName, testTblName, (short) -1);
    Assert.assertEquals(4, testPartitions.size());
    Partition p1 = testPartitions.get(2);
    p1.getSd().setLocation("/tmp/some_other_location/part=p2/city=Seattle");
    Partition p2 = testPartitions.get(3);
    p2.getSd().setLocation("/tmp/some_other_location/part=p2/city=Phoenix");
    client.alter_partitions(dbName, testTblName, Arrays.asList(p1, p2));

    GetPartitionsRequest request = getGetPartitionsRequest();
    request.getProjectionSpec().setFieldList(Arrays.asList("values", "sd"));
    request.setDbName(dbName);
    request.setTblName(testTblName);

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);
    Assert.assertNotNull("Response should have returned partition specs",
        response.getPartitionSpec());
    Assert
        .assertEquals("We should have two partition specs", 2, response.getPartitionSpec().size());
    Assert.assertNotNull("One SharedSD spec is expected",
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec());
    Assert.assertNotNull("One composing spec is expected",
        response.getPartitionSpec().get(1).getPartitionList());

    PartitionSpecWithSharedSD partitionSpecWithSharedSD =
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
    Assert.assertNotNull("sd was requested but not returned", partitionSpecWithSharedSD.getSd());
    Assert.assertEquals("shared SD should have table location", table.getSd().getLocation(),
        partitionSpecWithSharedSD.getSd().getLocation());
    List<List<String>> expectedVals = new ArrayList<>(2);
    expectedVals.add(Arrays.asList("p1", "PaloAlto"));
    expectedVals.add(Arrays.asList("p1", "SanFrancisco"));

    for (int i=0; i<partitionSpecWithSharedSD.getPartitions().size(); i++) {
      PartitionWithoutSD retPartition = partitionSpecWithSharedSD.getPartitions().get(i);
      Assert.assertEquals(2, retPartition.getValuesSize());
      validateList(expectedVals.get(i), retPartition.getValues());
      Assert.assertNull("parameters were not requested so should have been null",
          retPartition.getParameters());
    }

    PartitionListComposingSpec composingSpec =
        response.getPartitionSpec().get(1).getPartitionList();
    Assert.assertNotNull("composing spec should have returned 2 partitions",
        composingSpec.getPartitions());
    Assert.assertEquals("composing spec should have returned 2 partitions", 2,
        composingSpec.getPartitionsSize());

    expectedVals.clear();
    expectedVals.add(Arrays.asList("p2", "Phoenix"));
    expectedVals.add(Arrays.asList("p2", "Seattle"));
    for (int i=0; i<composingSpec.getPartitions().size(); i++) {
      Partition partition = composingSpec.getPartitions().get(i);
      Assert.assertEquals(2, partition.getValuesSize());
      validateList(expectedVals.get(i), partition.getValues());
      Assert.assertNull("parameters were not requested so should have been null",
          partition.getParameters());
    }
  }

  @Test
  public void testGetPartitionsWithFilterExpr() throws TException {
    runGetPartitionsUsingExpr();
  }

  @Test
  public void testGetPartitionsUsingNames() throws Exception {
    runGetPartitionsUsingNames();
  }

  @Test
  public void testGetPartitionsUsingValues() throws Exception {
    runGetPartitionsUsingVals();
  }

  @Test
  public void testGetPartitionsUsingExprWithJDO() throws Exception {
    // disable direct SQL to make sure
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL.getVarname(), "false");
    runGetPartitionsUsingExpr();
  }

  @Test
  public void testGetPartitionsUsingValuesWithJDO() throws Exception {
    // disable direct SQL to make sure
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL.getVarname(), "false");
    runGetPartitionsUsingVals();
  }

  @Test
  public void testGetPartitionsUsingNamesWithJDO() throws Exception {
    // disable direct SQL to make sure
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL.getVarname(), "false");
    runGetPartitionsUsingNames();
  }

  @Test(expected = MetaException.class)
  public void testInvalidFilterByNames() throws Exception {
    runWithInvalidFilterByNames();
  }

  @Test(expected = MetaException.class)
  public void testInvalidFilterByNamesWithJDO() throws Exception {
    // disable direct SQL to make sure
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL.getVarname(), "false");
    runWithInvalidFilterByNames();
  }

  @Test(expected = MetaException.class)
  public void testInvalidProjectFieldNames() throws TException {
    runWithInvalidFieldNames(Arrays.asList("values", "invalid.field.name"));
  }

  @Test(expected = MetaException.class)
  public void testInvalidProjectFieldNames2() throws TException {
    runWithInvalidFieldNames(Arrays.asList(""));
  }

  @Test(expected = MetaException.class)
  public void testInvalidProjectFieldNamesWithJDO() throws TException {
    // disable direct SQL to make sure
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL.getVarname(), "false");
    runWithInvalidFieldNames(Arrays.asList("values", "invalid.field.name"));
  }

  @Test(expected = MetaException.class)
  public void testInvalidProjectFieldNames2WithJDO() throws TException {
    // disable direct SQL to make sure
    client.setMetaConf(ConfVars.TRY_DIRECT_SQL.getVarname(), "false");
    runWithInvalidFieldNames(Arrays.asList(""));
  }

  private void runWithInvalidFilterByNames() throws TException {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec.setFieldList(Arrays.asList("sd.location"));
    request.getFilterSpec().setFilterMode(PartitionFilterMode.BY_NAMES);
    // filter mode is set but not filters are provided
    client.getPartitionsWithSpecs(request);
  }

  private void runWithInvalidFieldNames(List<String> values) throws TException {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec.setFieldList(values);
    client.getPartitionsWithSpecs(request);
  }

  private void runGetPartitionsUsingExpr() throws TException {
    // test simple case first
    getPartitionsWithExpr(Arrays.asList("state=\"CA\""), 2);
    // Logical AND in filter
    getPartitionsWithExpr(Arrays.asList("state=\"CA\" AND city=\"PaloAlto\""), 1);
    // empty result set
    getPartitionsWithExpr(Arrays.asList("state=\"CA\" AND city=\"Seattle\""), 0);

    // NOT operator
    getPartitionsWithExpr(Arrays.asList("state=\"CA\" AND city !=\"PaloAlto\""), 1);
    // nested expr
    getPartitionsWithExpr(Arrays
            .asList("(state=\"CA\" AND city !=\"PaloAlto\") OR (state=\"WA\" AND city = \"Seattle\")"),
        2);

    // multiple filters
    getPartitionsWithExpr(Arrays.asList("state=\"CA\"", "city=\"PaloAlto\""), 1);
    getPartitionsWithExpr(
        Arrays.asList("state=\"CA\" OR state=\"WA\"", "city=\"PaloAlto\" OR city=\"Seattle\""), 2);
    // test empty result
    getPartitionsWithExpr(Arrays.asList("state=\"AZ\"", "city=\"Tucson\""), 0);
  }

  private void getPartitionsWithExpr(List<String> filters, int expectedPartition) throws TException {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec.setFieldList(Arrays.asList("sd.location"));
    request.getFilterSpec().setFilterMode(PartitionFilterMode.BY_EXPR);
    request.getFilterSpec().setFilters(filters);

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);
    Assert.assertNotNull(response);
    if (expectedPartition > 0) {
      PartitionSpecWithSharedSD partitionSpecWithSharedSD =
          response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
      Assert.assertNotNull(partitionSpecWithSharedSD);
      Assert.assertEquals("Invalid number of partitions returned", expectedPartition,
          partitionSpecWithSharedSD.getPartitionsSize());
    } else {
      Assert.assertTrue(
          "Partition spec should have been empty since filter doesn't match with any partitions",
          response.getPartitionSpec().isEmpty());
    }
  }

  private void getPartitionsWithVals(List<String> filters, int expectedPartitions)
      throws TException {
    // get partitions from "trusted" API
    List<Partition> partitions = client.listPartitions(dbName, tblName, filters, (short) -1);
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec.setFieldList(Arrays.asList("sd.location"));
    request.getFilterSpec().setFilterMode(PartitionFilterMode.BY_VALUES);
    request.getFilterSpec().setFilters(filters);

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);
    Assert.assertNotNull(response);
    if (expectedPartitions > 0) {
      PartitionSpecWithSharedSD partitionSpecWithSharedSD =
          response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
      Assert.assertNotNull(partitionSpecWithSharedSD);
      Assert.assertEquals("Invalid number of partitions returned", expectedPartitions,
          partitionSpecWithSharedSD.getPartitionsSize());
      verifyLocations(partitions, partitionSpecWithSharedSD.getSd(),
          partitionSpecWithSharedSD.getPartitions());
    } else {
      Assert.assertTrue(
          "Partition spec should have been empty since filter doesn't match with any partitions",
          response.getPartitionSpec().isEmpty());
    }
  }

  private void runGetPartitionsUsingVals() throws TException {
    // top level val set
    getPartitionsWithVals(Arrays.asList("CA"), 2);
    // exactly one partition
    getPartitionsWithVals(Arrays.asList("CA", "PaloAlto"), 1);
    // non-existing partition should return zero partitions
    getPartitionsWithVals(Arrays.asList("CA", "CityDoesNotExist"), 0);
  }

  private void getPartitionsWithNames(List<String> names, int expectedPartitionCount) throws TException {
    GetPartitionsRequest request = getGetPartitionsRequest();
    GetProjectionsSpec projectSpec = request.getProjectionSpec();
    projectSpec.setFieldList(Arrays.asList("sd.location"));
    request.getFilterSpec().setFilterMode(PartitionFilterMode.BY_NAMES);
    request.getFilterSpec().setFilters(names);

    GetPartitionsResponse response = client.getPartitionsWithSpecs(request);
    Assert.assertNotNull(response);
    if (expectedPartitionCount > 0) {
      PartitionSpecWithSharedSD partitionSpecWithSharedSD =
          response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
      Assert.assertNotNull(partitionSpecWithSharedSD);
      Assert.assertEquals("Invalid number of partitions returned", expectedPartitionCount,
          partitionSpecWithSharedSD.getPartitionsSize());
      List<Partition> origPartitions = client.getPartitionsByNames(dbName, tblName, names);
      verifyLocations(origPartitions, partitionSpecWithSharedSD.getSd(), partitionSpecWithSharedSD.getPartitions());
    } else {
      Assert.assertTrue(
          "Partition spec should have been empty since filter doesn't match with any partitions",
          response.getPartitionSpec().isEmpty());
    }
  }

  private void runGetPartitionsUsingNames() throws TException {
    List<String> names = client.listPartitionNames(dbName, tblName, (short) -1);
    // remove one to make sure that the test is really looking at 3 names
    names.remove(names.size() - 1);
    getPartitionsWithNames(names, 3);
    // filter mode is set. Empty filter names. So no partitions should be returned
    getPartitionsWithNames(Arrays.asList(""), 0);
    // invalid name
    getPartitionsWithNames(Arrays.asList("invalidPartitionName"), 0);
  }

  private void validateBasic(GetPartitionsResponse response) throws TException {
    Assert.assertNotNull("Response is null", response);
    Assert.assertNotNull("Returned partition spec is null", response.getPartitionSpec());
    Assert.assertEquals(1, response.getPartitionSpecSize());
    PartitionSpecWithSharedSD partitionSpecWithSharedSD =
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
    Assert.assertNotNull(partitionSpecWithSharedSD.getSd());
    StorageDescriptor sharedSD = partitionSpecWithSharedSD.getSd();
    Assert.assertEquals("Root location should be set to table location", tbl.getSd().getLocation(),
        sharedSD.getLocation());

    List<PartitionWithoutSD> partitionWithoutSDS = partitionSpecWithSharedSD.getPartitions();
    Assert.assertEquals(origPartitions.size(), partitionWithoutSDS.size());
    for (int i = 0; i < origPartitions.size(); i++) {
      Partition origPartition = origPartitions.get(i);
      PartitionWithoutSD returnedPartitionWithoutSD = partitionWithoutSDS.get(i);
      Assert.assertEquals(String.format("Location returned for Partition %d is not correct", i),
          origPartition.getSd().getLocation(),
          sharedSD.getLocation() + returnedPartitionWithoutSD.getRelativePath());
    }
  }

  private GetPartitionsRequest getGetPartitionsRequest() {
    GetPartitionsRequest request = new GetPartitionsRequest();
    request.setProjectionSpec(new GetProjectionsSpec());
    request.setFilterSpec(new GetPartitionsFilterSpec());
    request.setTblName(tblName);
    request.setDbName(dbName);
    return request;
  }

  private void verifyLocations(List<Partition> origPartitions, StorageDescriptor sharedSD,
      List<PartitionWithoutSD> partitionWithoutSDS) {
    int i=0;
    for (Partition origPart : origPartitions) {
      // in case of location sharedSD has the base location and partition has relative location
      Assert.assertEquals("Location does not match", origPart.getSd().getLocation(),
          sharedSD.getLocation() + partitionWithoutSDS.get(i).getRelativePath());
      Assert.assertNull("values were not requested but are still set",
          partitionWithoutSDS.get(i).getValues());
      Assert.assertNull("Parameters were not requested but are still set",
          partitionWithoutSDS.get(i).getParameters());
      i++;
    }
  }

  private <K, V> void validateMap(Map<K, V> aMap, Map<K, V> bMap) {
    if ((aMap == null || aMap.isEmpty()) && (bMap == null || bMap.isEmpty())) {
      return;
    }
    // Equality is verified here because metastore updates stats automatically
    // and adds them in the returned partition. So the returned partition will
    // have parameters + some more parameters for the basic stats
    Assert.assertTrue(bMap.size() >= aMap.size());
    for (Entry<K, V> entries : aMap.entrySet()) {
      Assert.assertTrue("Expected " + entries.getKey() + " is missing from the map",
          bMap.containsKey(entries.getKey()));
      Assert.assertEquals("Expected value to be " + aMap.get(entries.getKey()) + " found" + bMap
          .get(entries.getKey()), aMap.get(entries.getKey()), bMap.get(entries.getKey()));
    }
  }

  private <T> void validateList(List<T> aList, List<T> bList) {
    if ((aList == null || aList.isEmpty()) && (bList == null || bList.isEmpty())) {
      return;
    }
    Assert.assertEquals(aList.size(), bList.size());
    Iterator<T> origValuesIt = aList.iterator();
    Iterator<T> retValuesIt = bList.iterator();
    while (origValuesIt.hasNext()) {
      Assert.assertTrue(retValuesIt.hasNext());
      Assert.assertEquals(origValuesIt.next(), retValuesIt.next());
    }
  }
}
