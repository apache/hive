package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

public class MsckDesc extends DDLWork implements Serializable {

  private String tableName;
  private ArrayList<LinkedHashMap<String, String>> partSpecs;
  private Path resFile;
  private boolean repairPartitions;

  /**
   * Description of a msck command.
   * 
   * @param tableName
   *          Table to check, can be null.
   * @param partSpecs
   *          Partition specification, can be null.
   * @param resFile
   *          Where to save the output of the command
   * @param repairPartitions
   *          remove stale / add new partitions found during the check
   */
  public MsckDesc(String tableName, List<? extends Map<String, String>> partSpecs,
      Path resFile, boolean repairPartitions) {
    super();
    this.tableName = tableName;
    this.partSpecs = new ArrayList<LinkedHashMap<String, String>>(partSpecs.size());
    for (int i = 0; i < partSpecs.size(); i++) {
      this.partSpecs.add(new LinkedHashMap<String, String>(partSpecs.get(i))); 
    }
    this.resFile = resFile;
    this.repairPartitions = repairPartitions;
  }

  /**
   * @return the table to check
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the table to check
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return partitions to check.
   */
  public ArrayList<LinkedHashMap<String, String>> getPartSpecs() {
    return partSpecs;
  }

  /**
   * @param partitionSpec
   *          partitions to check.
   */
  public void setPartSpecs(ArrayList<LinkedHashMap<String, String>> partSpecs) {
    this.partSpecs = partSpecs;
  }

  /**
   * @return file to save command output to
   */
  public Path getResFile() {
    return resFile;
  }

  /**
   * @param resFile
   *          file to save command output to
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }

  /**
   * @return remove stale / add new partitions found during the check
   */
  public boolean isRepairPartitions() {
    return repairPartitions;
  }

  /**
   * @param remove
   *          stale / add new partitions found during the check
   */
  public void setRepairPartitions(boolean repairPartitions) {
    this.repairPartitions = repairPartitions;
  }
}
