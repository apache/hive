package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.CheckResult.PartitionResult;
import org.apache.hadoop.hive.conf.HiveConf;

import com.facebook.thrift.TException;

/**
 * Verify that the information in the metastore matches what
 * is on the filesystem. Return a CheckResult object
 * containing lists of missing and any unexpected tables and partitions.
 */
public class HiveMetaStoreChecker {

  public static final Log LOG = LogFactory.getLog(HiveMetaStoreChecker.class);

  private Hive hive;
  private HiveConf conf;

  public HiveMetaStoreChecker(Hive hive) {
    super();
    this.hive = hive;
    this.conf = hive.getConf();
  }

  /**
   * Check the metastore for inconsistencies, data missing in either the
   * metastore or on the dfs.
   * 
   * @param dbName
   *          name of the database, if not specified the default will be used.
   * @param tableName
   *          Table we want to run the check for. If null we'll check all the
   *          tables in the database.
   * @param partitions List of partition name value pairs, 
   * if null or empty check all partitions
   * @param result Fill this with the results of the check 
   * @throws HiveException Failed to get required information 
   * from the metastore.
   * @throws IOException Most likely filesystem related
   */
  public void checkMetastore(String dbName, String tableName,
      List<Map<String, String>> partitions, CheckResult result) 
    throws HiveException, IOException {

    if (dbName == null || "".equalsIgnoreCase(dbName)) {
      dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    }

    try {
      if (tableName == null || "".equals(tableName)) {
        // no table specified, check all tables and all partitions.
        List<String> tables = hive.getTablesForDb(dbName, ".*");
        for (String currentTableName : tables) {
          checkTable(dbName, currentTableName, null, result);
        }
  
        findUnknownTables(dbName, tables, result);
      } else if (partitions == null || partitions.isEmpty()) {
        // only one table, let's check all partitions
        checkTable(dbName, tableName, null, result);
      } else {
        // check the specified partitions
        checkTable(dbName, tableName, partitions, result);
      }
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  /**
   * Check for table directories that aren't in the metastore.
   * @param dbName Name of the database
   * @param tables List of table names
   * @param result Add any found tables to this
   * @throws HiveException Failed to get required information 
   * from the metastore.
   * @throws IOException Most likely filesystem related
   * @throws MetaException Failed to get required information 
   * from the metastore.
   * @throws NoSuchObjectException Failed to get required information 
   * from the metastore.
   * @throws TException Thrift communication error.
   */
  void findUnknownTables(String dbName, List<String> tables,
      CheckResult result) throws IOException, MetaException, TException,
      HiveException {

    Set<Path> dbPaths = new HashSet<Path>();
    Set<String> tableNames = new HashSet<String>(tables);

    for (String tableName : tables) {
      Table table = hive.getTable(dbName, tableName);
      // hack, instead figure out a way to get the db paths
      String isExternal = table.getParameters().get("EXTERNAL");
      if (isExternal == null || !"TRUE".equalsIgnoreCase(isExternal)) {
        dbPaths.add(table.getPath().getParent());
      }
    }

    for (Path dbPath : dbPaths) {
      FileSystem fs = dbPath.getFileSystem(conf);
      FileStatus[] statuses = fs.listStatus(dbPath);
      for (FileStatus status : statuses) {
        
        if (status.isDir() 
            && !tableNames.contains(status.getPath().getName())) {
          
          result.getTablesNotInMs().add(status.getPath().getName());
        }
      }
    }
  }

  /**
   * Check the metastore for inconsistencies, data missing in either the
   * metastore or on the dfs.
   * 
   * @param dbName Name of the database
   * @param tableName Name of the table
   * @param partitions Partitions to check, if null or empty
   * get all the partitions.
   * @param result Result object
   * @throws HiveException Failed to get required information 
   * from the metastore.
   * @throws IOException Most likely filesystem related
   * @throws MetaException Failed to get required information 
   * from the metastore.
   */
  void checkTable(String dbName, String tableName,
      List<Map<String, String>> partitions, CheckResult result)
      throws MetaException, IOException, HiveException {

    Table table = null;

    try {
      table = hive.getTable(dbName, tableName);
    } catch (HiveException e) {
      result.getTablesNotInMs().add(tableName);
      return;
    }

    List<Partition> parts = new ArrayList<Partition>();
    boolean findUnknownPartitions = true;
    
    if (table.isPartitioned()) {
      if (partitions == null || partitions.isEmpty()) {
        // no partitions specified, let's get all
        parts = hive.getPartitions(table);
      } else {
        //we're interested in specific partitions,
        //don't check for any others
        findUnknownPartitions = false;
        for (Map<String, String> map : partitions) {
          Partition part = hive.getPartition(table, map, false);
          if(part == null) {
            PartitionResult pr = new PartitionResult();
            pr.setTableName(tableName);
            pr.setPartitionName(Warehouse.makePartName(map));
            result.getPartitionsNotInMs().add(pr);
          } else {
            parts.add(part);
          }
        }
      }
    }

    checkTable(table, parts, findUnknownPartitions, result);
  }

  /**
   * Check the metastore for inconsistencies, data missing in either the
   * metastore or on the dfs.
   * 
   * @param table Table to check
   * @param parts Partitions to check
   * @param result Result object
   * @param findUnknownPartitions Should we try to find unknown partitions?
   * @throws IOException Could not get information from filesystem
   * @throws HiveException Could not create Partition object
   */
  void checkTable(Table table, List<Partition> parts, 
      boolean findUnknownPartitions, CheckResult result) 
    throws IOException, HiveException {

    Path tablePath = table.getPath();
    FileSystem fs = tablePath.getFileSystem(conf);
    if (!fs.exists(tablePath)) {
      result.getTablesNotOnFs().add(table.getName());
      return;
    }

    Set<Path> partPaths = new HashSet<Path>();

    // check that the partition folders exist on disk
    for (Partition partition : parts) {
      if(partition == null) {
        //most likely the user specified an invalid partition
        continue;
      }
      Path partPath = partition.getPartitionPath();
      fs = partPath.getFileSystem(conf);
      if (!fs.exists(partPath)) {
        PartitionResult pr = new PartitionResult();
        pr.setPartitionName(partition.getName());
        pr.setTableName(partition.getTable().getName());
        result.getPartitionsNotOnFs().add(pr);
      }

      for (int i = 0; i < partition.getSpec().size(); i++) {
        partPaths.add(partPath.makeQualified(fs));
        partPath = partPath.getParent();
      }
    }

    if(findUnknownPartitions) {
      findUnknownPartitions(table, partPaths, result);
    }
  }

  /**
   * Find partitions on the fs that are
   * unknown to the metastore
   * @param table Table where the partitions would be located
   * @param partPaths Paths of the partitions the ms knows about
   * @param result Result object 
   * @throws IOException Thrown if we fail at fetching listings from
   * the fs.
   */
  void findUnknownPartitions(Table table, Set<Path> partPaths, 
      CheckResult result) throws IOException {
    
    Path tablePath = table.getPath();
    // now check the table folder and see if we find anything
    // that isn't in the metastore
    Set<Path> allPartDirs = new HashSet<Path>();
    getAllLeafDirs(tablePath, allPartDirs);
    // don't want the table dir
    allPartDirs.remove(tablePath);

    // remove the partition paths we know about
    allPartDirs.removeAll(partPaths);
    
    // we should now only have the unexpected folders left
    for (Path partPath : allPartDirs) {
      FileSystem fs = partPath.getFileSystem(conf);
      String partitionName = getPartitionName(fs.makeQualified(tablePath), 
          partPath);
      
      if (partitionName != null) {
        PartitionResult pr = new PartitionResult();
        pr.setPartitionName(partitionName);
        pr.setTableName(table.getName());
        
        result.getPartitionsNotInMs().add(pr);
      }
    }
  }

  /**
   * Get the partition name from the path.
   * 
   * @param tablePath Path of the table.
   * @param partitionPath Path of the partition.
   * @return Partition name, for example partitiondate=2008-01-01
   */
  private String getPartitionName(Path tablePath, Path partitionPath) {
    String result = null;
    Path currPath = partitionPath;
    while (currPath != null && !tablePath.equals(currPath)) {
      if(result == null) {
        result = currPath.getName();
      } else {
        result = currPath.getName() + Path.SEPARATOR + result;
      }
      
      currPath = currPath.getParent();
    }
    return result;
  }

  /**
   * Recursive method to get the leaf directories of a base path.
   * Example:
   * base/dir1/dir2
   * base/dir3
   * 
   * This will return dir2 and dir3 but not dir1.
   * 
   * @param basePath Start directory
   * @param allDirs This set will contain the leaf paths at the end.
   * @throws IOException Thrown if we can't get lists from the fs.
   */

  private void getAllLeafDirs(Path basePath, Set<Path> allDirs)
      throws IOException {
    getAllLeafDirs(basePath, allDirs, basePath.getFileSystem(conf));
  }

  private void getAllLeafDirs(Path basePath, Set<Path> allDirs, FileSystem fs)
      throws IOException {
    
    FileStatus[] statuses = fs.listStatus(basePath);

    if (statuses.length == 0) {
      allDirs.add(basePath);
    }

    for (FileStatus status : statuses) {
      if (status.isDir()) {
        getAllLeafDirs(status.getPath(), allDirs, fs);
      }
    }
  }

}
