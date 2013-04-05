package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.cube.metadata.BaseDimension;
import org.apache.hadoop.hive.ql.cube.metadata.ColumnMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.Cube;
import org.apache.hadoop.hive.ql.cube.metadata.CubeDimension;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.CubeMetastoreClient;
import org.apache.hadoop.hive.ql.cube.metadata.ExprMeasure;
import org.apache.hadoop.hive.ql.cube.metadata.HDFSStorage;
import org.apache.hadoop.hive.ql.cube.metadata.HierarchicalDimension;
import org.apache.hadoop.hive.ql.cube.metadata.InlineDimension;
import org.apache.hadoop.hive.ql.cube.metadata.ReferencedDimension;
import org.apache.hadoop.hive.ql.cube.metadata.Storage;
import org.apache.hadoop.hive.ql.cube.metadata.TableReference;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.TextInputFormat;

public class CubeTestSetup {

  private Cube cube;
  private Set<CubeMeasure> cubeMeasures;
  private Set<CubeDimension> cubeDimensions;
  private final String cubeName = "testCube";

  private void createCube(CubeMetastoreClient client) throws HiveException {
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
    client.createCube(cubeName, cubeMeasures, cubeDimensions);
  }

  private void createCubeFact(CubeMetastoreClient client) throws HiveException {
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
  }

  private void createDimWithTwoStorages(CubeMetastoreClient client)
      throws HiveException {
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
    snapshotDumpPeriods.put(hdfsStorage1, UpdatePeriod.HOURLY);
    snapshotDumpPeriods.put(hdfsStorage2, null);
    client.createCubeDimensionTable(dimName, dimColumns, dimensionReferences,
        snapshotDumpPeriods);
  }

  public void createSources() throws Exception {
    CubeMetastoreClient client =  CubeMetastoreClient.getInstance(
        new HiveConf(this.getClass()));
    createCube(client);
    createCubeFact(client);
    createDimWithTwoStorages(client);
  }

}
