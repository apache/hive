package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.SequenceFileInputFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapred.TextInputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCubeMetastoreClient {

  private CubeMetastoreClient client;

  //cube members
  private Cube cube;
  private Set<CubeMeasure> cubeMeasures;
  private Set<CubeDimension> cubeDimensions;
  private final String cubeName = "testCube";
  private Date now;

  @Before
  public void setup() throws HiveException {
    client =  CubeMetastoreClient.getInstance(new HiveConf(this.getClass()));
    now = new Date();

    defineCube();
  }

  @After
  public void teardown() {
    if (client != null) {
      client.close();
    }
  }

  private void defineCube() {
    cubeMeasures = new HashSet<CubeMeasure>();
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr1", "int",
        "first measure")));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr2", "float",
        "second measure"),
        null, "SUM", "RS"));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr3", "double",
        "third measure"),
        null, "MAX", null));
    cubeMeasures.add(new ColumnMeasure(new FieldSchema("msr4", "bigint",
        "fourth measure"),
        null, "COUNT", null));
    cubeMeasures.add(new ExprMeasure(new FieldSchema("msr5", "double",
        "fifth measure"),
        "avg(msr1 + msr2)"));
    cubeMeasures.add(new ExprMeasure(new FieldSchema("msr6", "bigint",
        "sixth measure"),
        "(msr1 + msr2)/ msr4", "", "SUM", "RS"));

    cubeDimensions = new HashSet<CubeDimension>();
    List<CubeDimension> locationHierarchy = new ArrayList<CubeDimension>();
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("zipcode",
        "int", "zip"), new TableReference("ziptable", "zipcode")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("cityid",
        "int", "city"), new TableReference("citytable", "id")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("stateid",
        "int", "state"), new TableReference("statetable", "id")));
    locationHierarchy.add(new ReferencedDimension(new FieldSchema("countryid",
        "int", "country"), new TableReference("countrytable", "id")));
    List<String> regions = Arrays.asList("APAC", "EMEA", "USA");
    locationHierarchy.add(new InlineDimension(new FieldSchema("regionname",
        "string", "region"), regions));

    cubeDimensions.add(new HierarchicalDimension("location", locationHierarchy));
    cubeDimensions.add(new BaseDimension(new FieldSchema("dim1", "string",
        "basedim")));
    cubeDimensions.add(new ReferencedDimension(
            new FieldSchema("dim2", "string", "ref dim"),
            new TableReference("testdim2", "id")));
    cubeDimensions.add(new InlineDimension(
            new FieldSchema("region", "string", "region dim"), regions));
    cube = new Cube(cubeName, cubeMeasures, cubeDimensions);
  }

  @Test
  public void testCube() throws Exception {
    client.createCube(cubeName, cubeMeasures, cubeDimensions);
    Assert.assertTrue(client.tableExists(cubeName));
    Table cubeTbl = client.getHiveTable(cubeName);
    Cube cube2 = new Cube(cubeTbl);
    Assert.assertTrue(cube.equals(cube2));
  }

  @Test
  public void testCubeFact() throws Exception {
    String factName = "testFact";
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add one dimension of the cube
    factColumns.add(new FieldSchema("zipcode","int", "zip"));

    Map<String, List<UpdatePeriod>> updatePeriods =
        new HashMap<String, List<UpdatePeriod>>();
    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, List<UpdatePeriod>>();
    List<UpdatePeriod> updates  = new ArrayList<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    storageAggregatePeriods.put(hdfsStorage, updates);
    updatePeriods.put(hdfsStorage.getName(), updates);

    CubeFactTable cubeFact = new CubeFactTable(cubeName, factName, factColumns,
        updatePeriods);

    // create cube fact
    client.createCubeFactTable(cubeName, factName, factColumns,
        storageAggregatePeriods);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFact.equals(cubeFact2));

    // Assert for storage tables
    for (Map.Entry<Storage, List<UpdatePeriod>> entry :
      storageAggregatePeriods.entrySet()) {
      List<UpdatePeriod> updatePeriodsList = entry.getValue();
      for (UpdatePeriod period : updatePeriodsList) {
        String storageTableName = MetastoreUtil.getFactStorageTableName(
            factName, period, entry.getKey().getPrefix());
        Assert.assertTrue(client.tableExists(storageTableName));
      }
    }

    // test partition
    client.addPartition(cubeFact, hdfsStorage, UpdatePeriod.HOURLY, now);
    Assert.assertTrue(client.factPartitionExists(cubeFact, hdfsStorage,
        UpdatePeriod.HOURLY, now));
  }

  @Test
  public void testCubeFactWithParts() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    String factNameWithPart = "testFactPart";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1","string", "dim1"));
    factColumns.add(new FieldSchema("dim2","string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("region","string", "region part"));

    Map<String, List<UpdatePeriod>> updatePeriods =
        new HashMap<String, List<UpdatePeriod>>();
    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, List<UpdatePeriod>>();
    List<UpdatePeriod> updates  = new ArrayList<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Storage hdfsStorageWithParts = new HDFSStorage("C1",
        SequenceFileInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorageWithParts.addToPartCols(factPartColumns.get(0));
    storageAggregatePeriods.put(hdfsStorageWithParts, updates);
    updatePeriods.put(hdfsStorageWithParts.getName(), updates);

    CubeFactTable cubeFactWithParts = new CubeFactTable(cubeName,
        factNameWithPart, factColumns, updatePeriods);
    client.createCubeFactTable(cubeName, factNameWithPart, factColumns,
        storageAggregatePeriods);
    Assert.assertTrue(client.tableExists(factNameWithPart));
    Table cubeTbl = client.getHiveTable(factNameWithPart);
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithParts.equals(cubeFact2));

    // Assert for storage tables
    for (Map.Entry<Storage, List<UpdatePeriod>> entry :
      storageAggregatePeriods.entrySet()) {
      List<UpdatePeriod> updatePeriodsList = entry.getValue();
      for (UpdatePeriod period : updatePeriodsList) {
        String storageTableName = MetastoreUtil.getFactStorageTableName(
            factNameWithPart, period, entry.getKey().getPrefix());
        Assert.assertTrue(client.tableExists(storageTableName));
      }
    }

    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    // test partition
    client.addPartition(cubeFactWithParts, hdfsStorageWithParts,
        UpdatePeriod.HOURLY, now, partSpec);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithParts,
        hdfsStorageWithParts,
        UpdatePeriod.HOURLY, now, partSpec));
  }

  @Test
  public void testCubeFactWithTwoStorages() throws Exception {
    List<FieldSchema> factColumns = new ArrayList<FieldSchema>(
        cubeMeasures.size());
    String factName = "testFactTwoStorages";

    for (CubeMeasure measure : cubeMeasures) {
      factColumns.add(measure.getColumn());
    }

    // add some dimensions of the cube
    factColumns.add(new FieldSchema("dim1","string", "dim1"));
    factColumns.add(new FieldSchema("dim2","string", "dim2"));

    List<FieldSchema> factPartColumns = new ArrayList<FieldSchema>();
    factPartColumns.add(new FieldSchema("region","string", "region part"));

    Map<String, List<UpdatePeriod>> updatePeriods =
        new HashMap<String, List<UpdatePeriod>>();
    Map<Storage, List<UpdatePeriod>> storageAggregatePeriods =
        new HashMap<Storage, List<UpdatePeriod>>();
    List<UpdatePeriod> updates  = new ArrayList<UpdatePeriod>();
    updates.add(UpdatePeriod.HOURLY);
    updates.add(UpdatePeriod.DAILY);
    Storage hdfsStorageWithParts = new HDFSStorage("C1",
        SequenceFileInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    hdfsStorageWithParts.addToPartCols(factPartColumns.get(0));
    Storage hdfsStorageWithNoParts = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());

    storageAggregatePeriods.put(hdfsStorageWithParts, updates);
    storageAggregatePeriods.put(hdfsStorageWithNoParts, updates);
    updatePeriods.put(hdfsStorageWithParts.getName(), updates);
    updatePeriods.put(hdfsStorageWithNoParts.getName(), updates);

    CubeFactTable cubeFactWithTwoStorages = new CubeFactTable(cubeName,
        factName, factColumns, updatePeriods);
    client.createCubeFactTable(cubeName, factName, factColumns,
        storageAggregatePeriods);
    Assert.assertTrue(client.tableExists(factName));
    Table cubeTbl = client.getHiveTable(factName);
    CubeFactTable cubeFact2 = new CubeFactTable(cubeTbl);
    Assert.assertTrue(cubeFactWithTwoStorages.equals(cubeFact2));

    // Assert for storage tables
    for (Map.Entry<Storage, List<UpdatePeriod>> entry :
      storageAggregatePeriods.entrySet()) {
      List<UpdatePeriod> updatePeriodsList = entry.getValue();
      for (UpdatePeriod period : updatePeriodsList) {
        String storageTableName = MetastoreUtil.getFactStorageTableName(
            factName, period, entry.getKey().getPrefix());
        Assert.assertTrue(client.tableExists(storageTableName));
      }
    }

    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(factPartColumns.get(0).getName(), "APAC");
    // test partition
    client.addPartition(cubeFactWithTwoStorages, hdfsStorageWithParts,
        UpdatePeriod.HOURLY, now, partSpec);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages,
        hdfsStorageWithParts,
        UpdatePeriod.HOURLY, now, partSpec));

    client.addPartition(cubeFactWithTwoStorages, hdfsStorageWithNoParts,
        UpdatePeriod.HOURLY, now);
    Assert.assertTrue(client.factPartitionExists(cubeFactWithTwoStorages,
        hdfsStorageWithNoParts,
        UpdatePeriod.HOURLY, now));
  }

  @Test
  public void testCubeDim() throws Exception {
    String dimName = "ziptable";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("zipcode", "int", "code"));
    dimColumns.add(new FieldSchema("f1", "string", "field1"));
    dimColumns.add(new FieldSchema("f2", "string", "field2"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));

    Map<String, TableReference> dimensionReferences =
        new HashMap<String, TableReference>();
    dimensionReferences.put("stateid", new TableReference("statetable", "id"));

    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    snapshotDumpPeriods.put(hdfsStorage, UpdatePeriod.HOURLY);
    dumpPeriods.put(hdfsStorage.getName(), UpdatePeriod.HOURLY);
    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, dimensionReferences, dumpPeriods);
    client.createCubeDimensionTable(dimName, dimColumns, dimensionReferences,
        snapshotDumpPeriods);
    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (Storage storage : snapshotDumpPeriods.keySet()) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          storage.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
    }

    // test partition
    client.addPartition(cubeDim, hdfsStorage, now);
    Assert.assertTrue(client.dimPartitionExists(cubeDim, hdfsStorage, now));
    Assert.assertTrue(client.latestPartitionExists(cubeDim, hdfsStorage));
  }

  @Test
  public void testCubeDimWithoutDumps() throws Exception {
    String dimName = "countrytable";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("capital", "string", "field2"));
    dimColumns.add(new FieldSchema("region", "string", "region name"));

    Map<String, TableReference> dimensionReferences =
        new HashMap<String, TableReference>();
    dimensionReferences.put("stateid", new TableReference("statetable", "id"));

    Storage hdfsStorage = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Set<Storage> storages = new HashSet<Storage>();
    storages.add(hdfsStorage);
    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, dimensionReferences);
    client.createCubeDimensionTable(dimName, dimColumns, dimensionReferences,
        storages);
    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    for (Storage storage : storages) {
      String storageTableName = MetastoreUtil.getDimStorageTableName(dimName,
          storage.getPrefix());
      Assert.assertTrue(client.tableExists(storageTableName));
      Assert.assertTrue(!client.getStorageTable(storageTableName).isPartitioned());
    }
  }

  @Test
  public void testCubeDimWithTwoStorages() throws Exception {
    String dimName = "citytable";

    List<FieldSchema>  dimColumns = new ArrayList<FieldSchema>();
    dimColumns.add(new FieldSchema("id", "int", "code"));
    dimColumns.add(new FieldSchema("name", "string", "field1"));
    dimColumns.add(new FieldSchema("stateid", "int", "state id"));

    Map<String, TableReference> dimensionReferences =
        new HashMap<String, TableReference>();
    dimensionReferences.put("stateid", new TableReference("statetable", "id"));

    Storage hdfsStorage1 = new HDFSStorage("C1",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Storage hdfsStorage2 = new HDFSStorage("C2",
        TextInputFormat.class.getCanonicalName(),
        HiveIgnoreKeyTextOutputFormat.class.getCanonicalName());
    Map<Storage, UpdatePeriod> snapshotDumpPeriods =
        new HashMap<Storage, UpdatePeriod>();
    Map<String, UpdatePeriod> dumpPeriods = new HashMap<String, UpdatePeriod>();
    snapshotDumpPeriods.put(hdfsStorage1, UpdatePeriod.HOURLY);
    dumpPeriods.put(hdfsStorage1.getName(), UpdatePeriod.HOURLY);
    snapshotDumpPeriods.put(hdfsStorage2, null);
    dumpPeriods.put(hdfsStorage2.getName(), null);
    CubeDimensionTable cubeDim = new CubeDimensionTable(dimName,
        dimColumns, dimensionReferences, dumpPeriods);
    client.createCubeDimensionTable(dimName, dimColumns, dimensionReferences,
        snapshotDumpPeriods);
    Assert.assertTrue(client.tableExists(dimName));
    Table cubeTbl = client.getHiveTable(dimName);
    CubeDimensionTable cubeDim2 = new CubeDimensionTable(cubeTbl);
    Assert.assertTrue(cubeDim.equals(cubeDim2));

    // Assert for storage tables
    String storageTableName1 = MetastoreUtil.getDimStorageTableName(dimName,
        hdfsStorage1.getPrefix());
    Assert.assertTrue(client.tableExists(storageTableName1));
    String storageTableName2 = MetastoreUtil.getDimStorageTableName(dimName,
        hdfsStorage2.getPrefix());
    Assert.assertTrue(client.tableExists(storageTableName2));
    Assert.assertTrue(!client.getStorageTable(storageTableName2).isPartitioned());
  }

}
