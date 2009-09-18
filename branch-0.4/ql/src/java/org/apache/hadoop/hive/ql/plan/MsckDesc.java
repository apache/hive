package org.apache.hadoop.hive.ql.plan;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

public class MsckDesc {

  private String tableName;
  private List<Map<String, String>> partitionSpec;
  private Path resFile;
  
  /**
   * Description of a msck command.
   * @param tableName Table to check, can be null.
   * @param partSpecs Partition specification, can be null. 
   * @param resFile Where to save the output of the command
   */
  public MsckDesc(String tableName, List<Map<String, String>> partSpecs, Path resFile) {
    super();
    this.tableName = tableName;
    this.partitionSpec = partSpecs;
    this.resFile = resFile;
  }

  /**
   * @return the table to check
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName the table to check
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return partitions to check.
   */
  public List<Map<String, String>> getPartitionSpec() {
    return partitionSpec;
  }

  /**
   * @param partitionSpec partitions to check.
   */
  public void setPartitionSpec(List<Map<String, String>> partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  /**
   * @return file to save command output to
   */
  public Path getResFile() {
    return resFile;
  }

  /**
   * @param resFile file to save command output to
   */
  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
  
}
