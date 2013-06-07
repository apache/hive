package org.apache.hadoop.hive.ql.cube.metadata;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
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

  /**
   * Get the instance of {@link CubeMetastoreClient} corresponding
   * to {@link HiveConf}
   *
   * @param conf
   * @return CubeMetastoreClient
   * @throws HiveException
   */
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

  /**
   * Close the current metastore client
   */
  public static void close() {
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
      for (Map.Entry<Storage, List<UpdatePeriod>> entry : storageAggregatePeriods.entrySet()) {
        updatePeriods.put(entry.getKey().getName(), entry.getValue());
      }
      return updatePeriods;
    } else {
      return null;
    }
  }

  /**
   * Create cube in metastore defined by {@link Cube} object
   *
   * @param cube the {@link Cube} object.
   * @throws HiveException
   */
  public void createCube(Cube cube) throws HiveException {
    createCubeHiveTable(cube);
  }

  /**
   * Create cube defined by measures and dimensions
   *
   * @param name Name of the cube
   * @param measures Measures of the cube
   * @param dimensions Dimensions of the cube
   *
   * @throws HiveException
   */
  public void createCube(String name, Set<CubeMeasure> measures,
      Set<CubeDimension> dimensions) throws HiveException {
    Cube cube = new Cube(name, measures, dimensions);
    createCube(cube);
  }

  /**
   * Create a cube fact table
   *
   * @param cubeName The cube name to which fact belongs to.
   * @param factName The fact name
   * @param columns The columns of fact table
   * @param storageAggregatePeriods Aggregate periods for the storages
   * @param weight Weight of the cube
   *
   * @throws HiveException
   */
  public void createCubeFactTable(String cubeName, String factName,
      List<FieldSchema> columns,
      Map<Storage, List<UpdatePeriod>> storageAggregatePeriods, double weight)
          throws HiveException {
    CubeFactTable factTable = new CubeFactTable(cubeName, factName, columns,
        getUpdatePeriods(storageAggregatePeriods), weight);
    createCubeTable(factTable, storageAggregatePeriods);
  }

  /**
   * Create a cube dimension table
   *
   * @param dimName dimensions name
   * @param columns Columns of the dimension table
   * @param weight Weight of the dimension table
   * @param dimensionReferences References to other dimensions
   * @param storages Storages on which dimension is available
   * @throws HiveException
   */
  public void createCubeDimensionTable(String dimName,
      List<FieldSchema> columns, double weight,
      Map<String, TableReference> dimensionReferences, Set<Storage> storages)
          throws HiveException {
    CubeDimensionTable dimTable = new CubeDimensionTable(dimName, columns,
        weight, getStorageNames(storages), dimensionReferences);
    createCubeTable(dimTable, storages);
  }

  private Set<String> getStorageNames(Set<Storage> storages) {
    Set<String> storageNames = new HashSet<String>();
    for (Storage storage : storages) {
      storageNames.add(storage.getName());
    }
    return storageNames;
  }

  private Map<String, UpdatePeriod> getDumpPeriods(
      Map<Storage, UpdatePeriod> storageDumpPeriods) {
    if (storageDumpPeriods != null) {
      Map<String, UpdatePeriod> updatePeriods =
          new HashMap<String, UpdatePeriod>();
      for (Map.Entry<Storage, UpdatePeriod> entry : storageDumpPeriods
          .entrySet()) {
        updatePeriods.put(entry.getKey().getName(), entry.getValue());
      }
      return updatePeriods;
    } else {
      return null;
    }
  }

  /**
   * Create a cube dimension table
   *
   * @param dimName dimensions name
   * @param columns Columns of the dimension table
   * @param weight Weight of the dimension table
   * @param dimensionReferences References to other dimensions
   * @param dumpPeriods Storages and their dump periods on which dimension
   *  is available
   * @throws HiveException
   */
  public void createCubeDimensionTable(String dimName,
      List<FieldSchema> columns, double weight,
      Map<String, TableReference> dimensionReferences,
      Map<Storage, UpdatePeriod> dumpPeriods)
          throws HiveException {
    // add date partitions for storages with dumpPeriods
    addDatePartitions(dumpPeriods);
    CubeDimensionTable dimTable = new CubeDimensionTable(dimName, columns,
        weight, getDumpPeriods(dumpPeriods), dimensionReferences);
    createCubeTable(dimTable, dumpPeriods.keySet());
  }

  private void addDatePartitions(Map<Storage, UpdatePeriod> dumpPeriods) {
    for (Map.Entry<Storage, UpdatePeriod> entry : dumpPeriods.entrySet()) {
      if (entry.getValue() != null) {
        entry.getKey().addToPartCols(Storage.getDatePartition());
      }
    }
  }

  /**
   * Create cube fact table defined by {@link CubeFactTable} object
   *
   * @param factTable The {@link CubeFactTable} object
   * @param storageAggregatePeriods Storages and their aggregate periods on
   *  which fact is available
   * @throws HiveException
   */
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

  /**
   * Create cube dimension table defined by {@link CubeDimensionTable} object
   *
   * @param dimTable The {@link CubeDimensionTable} object
   * @param storages Storages on which dimension is available
   * @throws HiveException
   */
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

  /**
   * Add storage to fact
   *
   * @param table The CubeFactTable
   * @param storage The storage
   * @param updatePeriods Update periods of the fact on the storage
   * @throws HiveException
   */
  public void addStorage(CubeFactTable table, Storage storage,
      List<UpdatePeriod> updatePeriods) throws HiveException {
    // TODO add the update periods to cube table properties
    createFactStorage(table.getName(), storage, getCubeTableSd(table),
        updatePeriods);
  }

  /**
   * Add an update period to a fact storage
   *
   * @param table The CubeFactTable
   * @param storage The storage
   * @param updatePeriod The Update period of the fact on the storage
   * @throws HiveException
   */
  public void addStorageUpdatePeriod(CubeFactTable table, Storage storage,
      UpdatePeriod updatePeriod) throws HiveException {
    // TODO add the update periods to cube table properties
    createFactStorageUpdatePeriod(table.getName(), storage,
        getStorageSD(storage, getCubeTableSd(table)), updatePeriod);
  }

  static List<String> getPartitionValues(Table tbl,
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

  /**
   * Add time partition to the fact on given storage for an updateperiod
   *
   * @param table The {@link CubeFactTable} object
   * @param storage The {@link Storage} object
   * @param updatePeriod The updatePeriod
   * @param partitionTimestamp partition timestamp
   * @throws HiveException
   */
  public void addPartition(CubeFactTable table, Storage storage,
      UpdatePeriod updatePeriod, Date partitionTimestamp)
          throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        table.getName(), updatePeriod, storage.getPrefix());
    addPartition(storageTableName, storage, getPartitionSpec(updatePeriod,
        partitionTimestamp), false);
  }

  /**
   * Add a partition to the fact on given storage for an updateperiod, with
   *  custom partition spec
   *
   * @param table The {@link CubeFactTable} object
   * @param storage The {@link Storage} object
   * @param updatePeriod The updatePeriod
   * @param partitionTimestamp partition timestamp
   * @param partSpec The partition spec
   * @throws HiveException
   */
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

  /**
   * Add a partition to dimension table on a give storage
   *
   * @param table The {@link CubeDimensionTable} object
   * @param storage The {@link Storage} object
   * @param partitionTimestamp
   * @throws HiveException
   */
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
      Date partitionTimestamp, Map<String, String> partSpec)
          throws HiveException {
    String storageTableName = MetastoreUtil.getFactStorageTableName(
        fact.getName(), updatePeriod, storage.getPrefix());
    return partitionExists(storageTableName, updatePeriod, partitionTimestamp,
        partSpec);
  }

  public boolean partitionExists(String storageTableName,
      UpdatePeriod updatePeriod,
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
        dim.getSnapshotDumpPeriods().get(storage.getName()),
        partitionTimestamp);
  }

  boolean latestPartitionExists(CubeDimensionTable dim,
      Storage storage) throws HiveException {
    String storageTableName = MetastoreUtil.getDimStorageTableName(
        dim.getName(), storage.getPrefix());
    return partitionExists(storageTableName, Storage.getLatestPartSpec());
  }

  /**
   * Get the hive {@link Table} corresponding to the name
   *
   * @param tableName
   * @return {@link Table} object
   * @throws HiveException
   */
  public Table getHiveTable(String tableName) throws HiveException {
    return getTable(tableName);
  }

  private Table getTable(String tableName) throws HiveException {
    Table tbl;
    try {
      tbl = getClient().getTable(tableName.toLowerCase());
    } catch (HiveException e) {
      e.printStackTrace();
      throw new HiveException("Could not get table", e);
    }
    return tbl;
  }

  /**
   * Is the table name passed a fact table?
   *
   * @param tableName table name
   * @return true if it is cube fact, false otherwise
   * @throws HiveException
   */
  public boolean isFactTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isFactTable(tbl);
  }

  boolean isFactTable(Table tbl) {
    String tableType = tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.FACT.name().equals(tableType);
  }

  boolean isFactTableForCube(Table tbl, Cube cube) {
    if (isFactTable(tbl)) {
      String cubeName = tbl.getParameters().get(
          MetastoreUtil.getFactCubeNameKey(tbl.getTableName()));
      return cubeName.equalsIgnoreCase(cube.getName());
    }
    return false;
  }

  /**
   * Is the table name passed a dimension table?
   *
   * @param tableName table name
   * @return true if it is cube dimension, false otherwise
   * @throws HiveException
   */
  public boolean isDimensionTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isDimensionTable(tbl);
  }

  boolean isDimensionTable(Table tbl) throws HiveException {
    String tableType = tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.DIMENSION.name().equals(tableType);
  }

  /**
   * Is the table name passed a cube?
   *
   * @param tableName table name
   * @return true if it is cube, false otherwise
   * @throws HiveException
   */
  public boolean isCube(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return isCube(tbl);
  }

  /**
   * Is the hive table a cube table?
   *
   * @param tbl
   * @return
   * @throws HiveException
   */
  boolean isCube(Table tbl) throws HiveException {
    String tableType = tbl.getParameters().get(
        MetastoreConstants.TABLE_TYPE_KEY);
    return CubeTableType.CUBE.name().equals(tableType);
  }

  /**
   * Get {@link CubeFactTable} object corresponding to the name
   *
   * @param tableName The cube fact name
   * @return Returns CubeFactTable if table name passed is a fact table,
   *  null otherwise
   * @throws HiveException
   */

  public CubeFactTable getFactTable(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    return getFactTable(tbl);
  }

  private CubeFactTable getFactTable(Table tbl) throws HiveException {
    if (isFactTable(tbl)) {
      return new CubeFactTable(tbl);
    }
    return null;
  }

  /**
   * Get {@link CubeDimensionTable} object corresponding to the name
   *
   * @param tableName The cube dimension name
   * @return Returns CubeDimensionTable if table name passed is a dimension
   *  table, null otherwise
   * @throws HiveException
   */
  public CubeDimensionTable getDimensionTable(String tableName)
      throws HiveException {
    Table tbl = getTable(tableName);
    if (isDimensionTable(tableName)) {
      return new CubeDimensionTable(tbl);
    }
    return null;
  }

  /**
   * Get {@link Cube} object corresponding to the name
   *
   * @param tableName The cube name
   * @return Returns cube is table name passed is a cube, null otherwise
   * @throws HiveException
   */
  public Cube getCube(String tableName) throws HiveException {
    Table tbl = getTable(tableName);
    if (isCube(tableName)) {
      return new Cube(tbl);
    }
    return null;
  }

  /**
   * Get all dimension tables in metastore
   *
   * @return List of dimension tables
   *
   * @throws HiveException
   */
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

  /**
   * Get all fact tables in the cube.
   *
   * @param cube Cube object
   *
   * @return List of fact tables
   * @throws HiveException
   */
  public List<CubeFactTable> getAllFactTables(Cube cube) throws HiveException {
    List<CubeFactTable> factTables = new ArrayList<CubeFactTable>();
    try {
      for (String tableName : getClient().getAllTables()) {
        Table tbl = getTable(tableName);
        if (isFactTableForCube(tbl, cube)) {
          factTables.add(new CubeFactTable(tbl));
        }
      }
    } catch (HiveException e) {
      throw new HiveException("Could not get all tables", e);
    }
    return factTables;
  }
}
