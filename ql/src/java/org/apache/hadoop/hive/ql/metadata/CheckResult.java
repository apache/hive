package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.List;

/**
 * Result class used by the HiveMetaStoreChecker.
 */
public class CheckResult {

  private List<String> tablesNotOnFs = new ArrayList<String>();
  private List<String> tablesNotInMs = new ArrayList<String>();
  private List<PartitionResult> partitionsNotOnFs = new ArrayList<PartitionResult>();
  private List<PartitionResult> partitionsNotInMs = new ArrayList<PartitionResult>();
  
  /**
   * @return a list of tables not found on the filesystem.
   */
  public List<String> getTablesNotOnFs() {
    return tablesNotOnFs;
  }
  
  /**
   * @param tablesNotOnFs a list of tables not found on the filesystem.
   */
  public void setTablesNotOnFs(List<String> tablesNotOnFs) {
    this.tablesNotOnFs = tablesNotOnFs;
  }
  
  /**
   * @return a list of tables not found in the metastore.
   */
  public List<String> getTablesNotInMs() {
    return tablesNotInMs;
  }
  
  /**
   * @param tablesNotInMs a list of tables not found in the metastore.
   */
  public void setTablesNotInMs(List<String> tablesNotInMs) {
    this.tablesNotInMs = tablesNotInMs;
  }
  
  /**
   * @return a list of partitions not found on the fs
   */
  public List<PartitionResult> getPartitionsNotOnFs() {
    return partitionsNotOnFs;
  }
  
  /**
   * @param partitionsNotOnFs a list of partitions not found on the fs
   */
  public void setPartitionsNotOnFs(List<PartitionResult> partitionsNotOnFs) {
    this.partitionsNotOnFs = partitionsNotOnFs;
  }
  
  /**
   * @return a list of partitions not found in the metastore
   */
  public List<PartitionResult> getPartitionsNotInMs() {
    return partitionsNotInMs;
  }
  
  /**
   * @param partitionsNotInMs a list of partitions not found in the metastore
   */
  public void setPartitionsNotInMs(List<PartitionResult> partitionsNotInMs) {
    this.partitionsNotInMs = partitionsNotInMs;
  } 
  
  /**
   * A basic description of a partition that is 
   * missing from either the fs or the ms.
   */
  public static class PartitionResult implements Comparable<PartitionResult> {
    private String partitionName;
    private String tableName;
    
    /**
     * @return name of partition
     */
    public String getPartitionName() {
      return partitionName;
    }
    
    /**
     * @param partitionName name of partition
     */
    public void setPartitionName(String partitionName) {
      this.partitionName = partitionName;
    }
    
    /**
     * @return table name
     */
    public String getTableName() {
      return tableName;
    }
    
    /**
     * @param tableName table name
     */
    public void setTableName(String tableName) {
      this.tableName = tableName;
    }
    
    public String toString() {
      return tableName + ":" + partitionName;
    }
    
    public int compareTo(PartitionResult o) {
      int ret = tableName.compareTo(o.tableName);
      return ret != 0 ? ret : partitionName.compareTo(o.partitionName);
    }
  }
  
}
