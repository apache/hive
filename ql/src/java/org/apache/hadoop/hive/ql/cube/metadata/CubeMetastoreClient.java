package org.apache.hadoop.hive.ql.cube.metadata;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * Wrapper class around Hive metastore to do cube metastore operations.
 *
 */
public class CubeMetastoreClient {
  private final Hive metastore;
  private final HiveConf config;

  private CubeMetastoreClient(HiveConf conf)
      throws HiveException {
    this.metastore = Hive.get(conf);
    this.config = conf;
  }

  private static final Map<HiveConf, CubeMetastoreClient> clientMapping =
      new HashMap<HiveConf, CubeMetastoreClient>();

  public static CubeMetastoreClient getInstance(HiveConf conf)
      throws HiveException {
    if (clientMapping.get(conf) == null) {
      clientMapping.put(conf, new CubeMetastoreClient(conf));
    }
    return clientMapping.get(conf);
  }

  private Hive getClient() {
    return metastore;
  }

  public void close() {
    Hive.closeCurrent();
  }

  private StorageDescriptor createStorageHiveTable(String tableName,
      StorageDescriptor sd,
      Map<String, String> parameters, TableType type,
      List<FieldSchema> partCols) throws HiveException {
    try {
      Table tbl = getClient().newTable(tableName.toLowerCase());
      tbl.getTTable().getParameters().putAll(parameters);
      tbl.getTTable().setSd(sd);
      if (partCols != null && partCols.size() != 0) {
        tbl.setPartCols(partCols);
      }
      tbl.setTableType(type);
      getClient().createTable(tbl);
      return tbl.getTTable().getSd();
    } catch (Exception e) {
      throw new HiveException("Exception creating table", e);
    }
  }

  private StorageDescriptor createCubeHiveTable(AbstractCubeTable table)
      throws HiveException {
    try {
      Table tbl = getClient().newTable(table.getName().toLowerCase());
      tbl.setTableType(TableType.MANAGED_TABLE);
      tbl.getTTable().getSd().setCols(table.getColumns());
      tbl.getTTable().getParameters().putAll(table.getProperties());
      getClient().createTable(tbl);
      return tbl.getTTable().getSd();
    } catch (Exception e) {
      throw new HiveException("Exception creating table", e);
    }
  }

  private void createFactStorage(String factName, Storage storage,
      StorageDescriptor parentSD, List<UpdatePeriod> updatePeriods)
          throws HiveException {
    for (UpdatePeriod updatePeriod : updatePeriods) {
      createFactStorageUpdatePeriod(factName, storage, parentSD, updatePeriod);
    }
  }

  private void createFactStorageUpdatePeriod(String factName, Storage storage,
      StorageDescriptor parentSD, UpdatePeriod updatePeriod)
          throws HiveException {
    String storageTblName = MetastoreUtil.getFactStorageTableName(factName,
        updatePeriod, storage.getPrefix());
    createStorage(storageTblName, storage, parentSD);
  }

  private void createDimStorage(String dimName, Storage storage,
      StorageDescriptor parentSD)
          throws HiveException {
    String storageTblName = MetastoreUtil.getDimStorageTableName(dimName,
        storage.getPrefix());
    createStorage(storageTblName, storage, parentSD);
  }

  private StorageDescriptor getStorageSD(Storage storage,
      StorageDescriptor parentSD) throws HiveException {
    StorageDescriptor physicalSd = new StorageDescriptor(parentSD);
    storage.setSD(physicalSd);
    return physicalSd;
  }

  private StorageDescriptor getCubeTableSd(AbstractCubeTable table)
      throws HiveException {
    Table cubeTbl = getTable(table.getName());
    return cubeTbl.getTTable().getSd();
  }

  private void createStorage(String name,
      Storage storage, StorageDescriptor parentSD) throws HiveException {
    StorageDescriptor physicalSd = getStorageSD(storage, parentSD);
    createStorageHiveTable(name,
        physicalSd, storage.getTableParameters(),
        storage.getTableType(), storage.getPartCols());
  }

  private Map<String, List<UpdatePeriod>> getUpdatePeriods(
      Map<Storage, List<UpdatePeriod>> storageAggregatePeriods) {
    if (storageAggregatePeriods != null) {
      Map<String, List<UpdatePeriod>> updatePeriods =
          new HashMap<String, List<UpdatePeriod>>();
      for (Map.Entry<Storage, List<UpdatePeriod>> entry :
        storageAggregatePeriods.entrySet()) {
        updatePeriods.put(entry.getKey().getName(), entry.getValue());
      }
      return updatePeriods;
    } else {
      return null;
    }
  }

  public void createCube(Cube cube) throws HiveException {
    createCubeHiveTable(cube);
  }

  public void createCube(String name, Set<CubeMeasure> measures,
      Set<CubeDimension> dimensions) throws HiveException {
    Cube cube = new Cube(name, measures, dimensions);
    createCube(cube);
  }

  public void createCubeFactTable(String cubeName, String factName,
      List<FieldSchema> columns,
      Map<Storage, List<UpdatePeriod>> storageAggregatePeriods)
          throws HiveException {
    CubeFactTable factTable = new CubeFactTable(cubeName, factName, columns,
        getUpdatePeriods(storageAggregatePeriods));
    createCubeTable(factTable, storageAggregatePeriods);
  }

  public void createCubeDimensionTable(String dimName,
      List<FieldSchema> columns,
      Map<String, TableReference> dimensionReferences, Set<Storage> storages)
          throws HiveException {
    CubeDimensionTable dimTable = new CubeDimensionTable(dimName, columns,
        dimensionReferences);
    createCubeTable(dimTable, storages);
  }

  private Map<String, UpdatePeriod> getDumpPeriods(
      Map<Storage, UpdatePeriod> storageDumpPeriods) {
    if (storageDumpPeriods != null) {
      Map<String, UpdatePeriod> updatePeriods = new HashMap<String, UpdatePeriod>();
      for (Map.Entry<Storage, UpdatePeriod> entry : storageDumpPeriods.entrySet()) {
        updatePeriods.put(entry.getKey().getName(), entry.getValue());
      }
      return updatePeriods;
    } else {
      return null;
    }
  }

  public void createCubeDimensionTable(String dimName,
      List<FieldSchema> columns,
      Map<String, TableReference> dimensionReferences,
      Map<Storage, UpdatePeriod> dumpPeriods)
          throws HiveException {
    // add date partitions for storages with dumpPeriods
    addDatePartitions(dumpPeriods);
    CubeDimensionTable dimTable = new CubeDimensionTable(dimName, columns,
        dimensionReferences, getDumpPeriods(dumpPeriods));
    createCubeTable(dimTable, dumpPeriods.keySet());
  }

  private void addDatePartitions(Map<Storage, UpdatePeriod> dumpPeriods) {
    for (Map.Entry<Storage, UpdatePeriod> entry : dumpPeriods.entrySet()) {
      if (entry.getValue() != null) {
        entry.getKey().addToPartCols(Storage.getDatePartition());
      }
    }
  }

  public void createCubeTable(CubeFactTable factTable,
      Map<Storage, List<UpdatePeriod>> storageAggregatePeriods)
          throws HiveException {
    // create virtual cube table in metastore
    StorageDescriptor sd = createCubeHiveTable(factTable);

    if (storageAggregatePeriods != null) {
      // create tables for each storage
      for (Storage storage : storageAggregatePeriods.keySet()) {
        // Add date partition for all facts.
        storage.addToPartCols(Storage.getDatePartition());
        createFactStorage(factTable.getName(), storage, sd,
            storageAggregatePeriods.get(storage));
      }
    }
  }

  public void createCubeTable(CubeDimensionTable dimTable,
      Set<Storage> storages) throws HiveException {
    // create virtual cube table in metastore
    StorageDescriptor sd = createCubeHiveTable(dimTable);

    if (storages != null) {
      // create tables for each storage
      for (Storage storage : storages) {
        createDimStorage(dimTable.getName(), storage, sd);
      }
    }
  }

  public void addStorage(CubeFactTable table, Storage storage,
      List<UpdatePeriod> updatePeriods) throws HiveException {
    //TODO add the update periods to cube table properties
    createFactStorage(table.getName(), storage, getCubeTableSd(table),
        updatePeriods);
  }

  public void addStorageUpdatePeriod(CubeFactTable table, Storage storage,
      UpdatePeriod updatePeriod) throws HiveException {
    //TODO add the update periods to cube table properties
    createFactStorageUpdatePeriod(table.getName(),storage,
        getStorageSD(storage, getCubeTableSd(table)), updatePeriod);
  }

  public void addColumn(AbstractCubeTable table, FieldSchema column) {
    //TODO
  }

  public void addDimensionReference(AbstractCubeTable srcTable, String srcCol,
      TableReference reference) {
    //TODO
  }

  //public void addMeasure(CubeFactTable table, Measure measure) {
  //TODO
  //}

  public void addUpdatePeriod(CubeFactTable table, UpdatePeriod updatePeriod) {
    //TODO
  }

  public static List<String> getPartitionValues(Table tbl,
      Map<String, String> partSpec) throws HiveException {
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartitionKeys()) {
      String val = partSpec.get(field.getName());
      if (val == null) {
        throw new HiveException("partition spec is invalid. field.getName()" +
            " does not exist in input.");
      }
      pvals.add(val);
    }
    return pvals;
  }

  public void addPartition(CubeFactTable table, Storage storage,
      UpdatePeriod updatePeriod, Date partitionTimestamp)
          throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        table.getName(), updatePeriod, storage.getPrefix());
    addPartition(storageTableName, storage, getPartitionSpec(updatePeriod,
        partitionTimestamp), false);
  }

  public void addPartition(CubeFactTable table, Storage storage,
      UpdatePeriod updatePeriod, Date partitionTimestamp,
      Map<String, String> partSpec)
          throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        table.getName(), updatePeriod, storage.getPrefix());
    partSpec.putAll(getPartitionSpec(updatePeriod,
        partitionTimestamp));
    addPartition(storageTableName, storage, partSpec, false);
  }

  public void addPartition(CubeDimensionTable table, Storage storage,
      Date partitionTimestamp) throws HiveException {
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        table.getName(), storage.getPrefix());
    addPartition(storageTableName, storage, getPartitionSpec(table.
        getSnapshotDumpPeriods().get(storage.getName()), partitionTimestamp),
        true);
  }

  private Map<String, String> getPartitionSpec(
      UpdatePeriod updatePeriod, Date partitionTimestamp) {
    Map<String, String> partSpec = new HashMap<String, String>();
    SimpleDateFormat dateFormat = new SimpleDateFormat(updatePeriod.format());
    String pval = dateFormat.format(partitionTimestamp);
    partSpec.put(Storage.getDatePartitionKey(), pval);
    return partSpec;
  }

  private void addPartition(String storageTableName, Storage storage,
      Map<String, String> partSpec, boolean makeLatest) throws HiveException {
    storage.addPartition(storageTableName, partSpec, config, makeLatest);
  }

  boolean tableExists(String cubeName)
      throws HiveException {
    try {
      return (getClient().getTable(cubeName.toLowerCase(), false) != null);
    } catch (HiveException e) {
      throw new HiveException("Could not check whether table exists", e);
    }
  }

  boolean factPartitionExists(CubeFactTable fact,
      Storage storage, UpdatePeriod updatePeriod,
      Date partitionTimestamp) throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        fact.getName(), updatePeriod, storage.getPrefix());
    return partitionExists(storageTableName, updatePeriod, partitionTimestamp);
  }

  boolean factPartitionExists(CubeFactTable fact,
      Storage storage, UpdatePeriod updatePeriod,
      Date partitionTimestamp, Map<String, String> partSpec) throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        fact.getName(), updatePeriod, storage.getPrefix());
    return partitionExists(storageTableName, updatePeriod, partitionTimestamp, partSpec);
  }

  boolean partitionExists(String storageTableName, UpdatePeriod updatePeriod,
      Date partitionTimestamp)
          throws HiveException {
    return partitionExists(storageTableName,
        getPartitionSpec(updatePeriod, partitionTimestamp));
  }

  boolean partitionExists(String storageTableName, UpdatePeriod updatePeriod,
      Date partitionTimestamp, Map<String, String> partSpec)
          throws HiveException {
    partSpec.putAll(getPartitionSpec(updatePeriod, partitionTimestamp));
    return partitionExists(storageTableName, partSpec);
  }

  private boolean partitionExists(String storageTableName,
      Map<String, String> partSpec) throws HiveException {
    try {
      Table storageTbl = getTable(storageTableName);
      Partition p = getClient().getPartition(storageTbl, partSpec, false);
      return (p != null && p.getTPartition() != null);
    } catch (HiveException e) {
      throw new HiveException("Could not check whether table exists", e);
    }
  }

  boolean dimPartitionExists(CubeDimensionTable dim,
      Storage storage, Date partitionTimestamp) throws HiveException {
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        dim.getName(), storage.getPrefix());
    return partitionExists(storageTableName,
        dim.getSnapshotDumpPeriods().get(storage.getName()), partitionTimestamp);
  }

  boolean latestPartitionExists(CubeDimensionTable dim,
      Storage storage) throws HiveException {
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        dim.getName(), storage.getPrefix());
    return partitionExists(storageTableName, Storage.getLatestPartSpec());
  }

  public Table getHiveTable(String tableName) throws HiveException {
    return getTable(tableName);
  }

  public Table getStorageTable(String tableName) throws HiveException {
    return getHiveTable(tableName);
  }

  private Table getTable(String tableName)  throws HiveException {
    Table tbl;
    try {
      tbl = getClient().getTable(tableName.toLowerCase());
    } catch (HiveException e) {
      e.printStackTrace();
      throw new HiveException("Could not get table", e);
    }
    return tbl;
  }

  public boolean isFactTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    String tableType = tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.FACT.equals(tableType);
  }

  public boolean isDimensionTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    String tableType = tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.DIMENSION.equals(tableType);
  }

  public boolean isCube(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    String tableType = tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.CUBE.equals(tableType);
  }

  public CubeFactTable getFactTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    if (CubeTableType.FACT.equals(tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY))) {
      return new CubeFactTable(tbl);
    }
    return null;
  }

  public CubeDimensionTable getDimensionTable(String tableName)
      throws HiveException {
    Table tbl = getTable(tableName);
    if (CubeTableType.DIMENSION.equals(tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY))) {
      return new CubeDimensionTable(tbl);
    }
    return null;
  }

  public Cube getCube(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    if (CubeTableType.CUBE.equals(tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY))) {
      return new Cube(tbl);
    }
    return null;
  }

  public List<CubeDimensionTable> getAllDimensionTables()
      throws HiveException {
    List<CubeDimensionTable> dimTables = new ArrayList<CubeDimensionTable>();
    try {
      for (String table : getClient().getAllTables()) {
        if (isDimensionTable(table)) {
          dimTables.add(getDimensionTable(table));
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all tables", e);
    }
    return dimTables;
  }

  public List<CubeFactTable> getAllFactTables() throws HiveException {
    List<CubeFactTable> factTables = new ArrayList<CubeFactTable>();
    try {
      for (String table : getClient().getAllTables()) {
        if (isFactTable(table)) {
          factTables.add(getFactTable(table));
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all tables", e);
    }
    return factTables;
  }

  public boolean isColumnInTable(String column, String table) {
    try {
      List<String> columns = getColumnNames(table);
      if (columns == null) {
        return false;
      } else {
        return columns.contains(column);
      }
    } catch (HiveException e) {
      e.printStackTrace();
      return false;
    }
  }

  private List<String> getColumnNames(String table) throws HiveException {
    List<FieldSchema> fields = getTable(table).getCols();
    List<String> columns = new ArrayList<String>(fields.size());
    for (FieldSchema f : fields) {
      columns.add(f.getName());
    }
    return columns;
  }

}
