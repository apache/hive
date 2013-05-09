package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;

public abstract class Storage implements Named {

  private final TableType tableType;
  private final Map<String, String> tableParameters =
      new HashMap<String, String>();
  private final List<FieldSchema> partCols = new ArrayList<FieldSchema>();
  protected Map<String, String> serdeParameters = new HashMap<String, String>();
  private final String name;

  protected Storage(String name, TableType type) {
    this.tableType = type;
    this.name = name;
  }

  public List<FieldSchema> getPartCols() {
    return partCols;
  }

  public TableType getTableType() {
    return tableType;
  }

  public Map<String, String> getTableParameters() {
    return tableParameters;
  }

  public void addToPartCols(FieldSchema column) {
    partCols.add(column);
  }

  protected void addToTableParameters(Map<String, String> parameters) {
    tableParameters.putAll(tableParameters);
  }

  protected void addTableProperty(String key, String value) {
    tableParameters.put(key, value);
  }

  public String getName() {
    return name;
  }

  public String getPrefix() {
    return getPrefix(getName());
  }

  public static String getPrefix(String name) {
    return name + StorageConstants.STORGAE_SEPERATOR;
  }

  public abstract void setSD(StorageDescriptor physicalSd) throws HiveException;

  public abstract void addPartition(String storageTableName,
      Map<String, String> partSpec, HiveConf conf, boolean makeLatest)
      throws HiveException;


  public static String getDatePartitionKey() {
    return StorageConstants.DATE_PARTITION_KEY;
  }

  private static Map<String, String> latestSpec = new HashMap<String, String>();
  static {
    latestSpec.put(getDatePartitionKey(),
        StorageConstants.LATEST_PARTITION_VALUE);
  }

  public static Map<String, String> getLatestPartSpec() {
    return latestSpec;
  }

  public static List<String> getPartitionsForLatest() {
    List<String> parts = new ArrayList<String>();
    parts.add(StorageConstants.LATEST_PARTITION_VALUE);
    return parts;
  }

  private static FieldSchema dtPart = new FieldSchema(getDatePartitionKey(),
      serdeConstants.STRING_TYPE_NAME,
      "date partition");

  public static FieldSchema getDatePartition() {
    return dtPart;
  }
}
