/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.util.StringUtils;

import org.apache.thrift.TException;

/**
 * The Hive class contains information about this instance of Hive.
 * An instance of Hive represents a set of data in a file system (usually HDFS)
 * organized for easy query processing
 *
 */

public class Hive {

  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Hive");
  static Hive db = null;
  
  private HiveConf conf = null;
  private ThreadLocal<IMetaStoreClient> threadLocalMSC = new ThreadLocal() {
    protected synchronized Object initialValue() {
        return null;
    }
    
    public synchronized void remove() {
      if( this.get() != null ) {
        ((IMetaStoreClient)this.get()).close();
      }
      super.remove();
    }
  };
  
  /**
   * Gets hive object for the current thread. If one is not initialized then a new one is created 
   * If the new configuration is different in metadata conf vars then a new one is created.
   * @param c new Hive Configuration
   * @return Hive object for current thread
   * @throws HiveException
   *
   */
  public static Hive get(HiveConf c) throws HiveException {
    boolean needsRefresh = false;

    if(db != null) {
      for(HiveConf.ConfVars oneVar: HiveConf.metaVars) {
        String oldVar = db.getConf().getVar(oneVar);
        String newVar = c.getVar(oneVar);
        if(oldVar.compareToIgnoreCase(newVar) != 0) {
          needsRefresh = true;
          break;
        }
      }
    }
    return get(c, needsRefresh);
  }

  /**
   * get a connection to metastore. see get(HiveConf) function for comments
   * @param c new conf
   * @param needsRefresh if true then creates a new one
   * @return The connection to the metastore
   * @throws HiveException
   */
  public static Hive get(HiveConf c, boolean needsRefresh) throws HiveException {
    if(db == null || needsRefresh) {
      closeCurrent();
      c.set("fs.scheme.class","dfs");
      db = new Hive(c);
    }
    return db;
  }

  public static Hive get() throws HiveException {
    if(db == null) {
      db = new Hive(new HiveConf(Hive.class));
    }
    return db;
  }
  
  public static void closeCurrent() {
    if(db != null) {
      db.close();
    }
  }
  
  /**
   * Hive
   *
   * @param argFsRoot
   * @param c
   *
   */
  private Hive(HiveConf c) throws  HiveException {
    this.conf = c;
  }
  
  /**
   * closes the connection to metastore for the calling thread
   */
  private void close() {
    LOG.info("Closing current thread's connection to Hive Metastore.");
    db.threadLocalMSC.remove();
  }
  
  /**
   * Creates a table metdata and the directory for the table data
   * @param tableName name of the table
   * @param columns list of fields of the table
   * @param partCols partition keys of the table
   * @param fileInputFormat Class of the input format of the table data file
   * @param fileOutputFormat Class of the output format of the table data file
   * @throws HiveException thrown if the args are invalid or if the metadata or the data directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns, List<String> partCols,
      Class<? extends InputFormat> fileInputFormat, Class<?> fileOutputFormat) throws HiveException {
    this.createTable(tableName, columns, partCols, fileInputFormat, fileOutputFormat, -1, null);
  }

  /**
   * Creates a table metdata and the directory for the table data
   * @param tableName name of the table
   * @param columns list of fields of the table
   * @param partCols partition keys of the table
   * @param fileInputFormat Class of the input format of the table data file
   * @param fileOutputFormat Class of the output format of the table data file
   * @param bucketCount number of buckets that each partition (or the table itself) should be divided into
   * @throws HiveException thrown if the args are invalid or if the metadata or the data directory couldn't be created
   */
  public void createTable(String tableName, List<String> columns, List<String> partCols,
      Class<? extends InputFormat> fileInputFormat, Class<?> fileOutputFormat, int bucketCount, List<String> bucketCols) throws HiveException {
    if(columns == null) {
      throw new HiveException("columns not specified for table " + tableName);
    }

    Table tbl = new Table(tableName);
    tbl.setInputFormatClass(fileInputFormat.getName());
    tbl.setOutputFormatClass(fileOutputFormat.getName());

    for (String col: columns) {
      FieldSchema field = new FieldSchema(col, org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME, "default");
      tbl.getCols().add(field);
    }

    if(partCols != null) {
      for (String partCol : partCols) {
        FieldSchema part = new FieldSchema();
        part.setName(partCol);
        part.setType(org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME); // default partition key
        tbl.getPartCols().add(part);
      }
    }
    tbl.setSerializationLib(LazySimpleSerDe.class.getName());
    tbl.setNumBuckets(bucketCount);
    tbl.setBucketCols(bucketCols);
    createTable(tbl);
  }


  /**
   * Updates the existing table metadata with the new metadata. 
   * @param tblName name of the existing table
   * @param newTbl new name of the table. could be the old name
   * @throws InvalidOperationException if the changes in metadata is not acceptable
   * @throws TException
   */
  public void alterTable(String tblName,
      Table newTbl) throws InvalidOperationException,
      HiveException {
    try {
      getMSC().alter_table(MetaStoreUtils.DEFAULT_DATABASE_NAME, tblName, newTbl.getTTable());
    } catch (MetaException e) {
      throw new HiveException("Unable to alter table.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter table.", e);
    }
  }

  /**
   * Creates the table with the give objects
   * @param tbl a table object
   * @throws HiveException
   */
  public void createTable(Table tbl) throws HiveException {
    createTable(tbl, false);
  }
  
  /**
   * Creates the table with the give objects
   * @param tbl a table object
   * @param ifNotExists if true, ignore AlreadyExistsException
   * @throws HiveException
   */
  public void createTable(Table tbl, boolean ifNotExists) throws HiveException {
    try {
      tbl.initSerDe();
      if(tbl.getCols().size() == 0) {
        tbl.setFields(MetaStoreUtils.getFieldsFromDeserializer(tbl.getName(), tbl.getDeserializer()));
      }
      tbl.checkValidity();
      getMSC().createTable(tbl.getTTable());
    } catch (AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Drops table along with the data in it. If the table doesn't exist
   * then it is a no-op
   * @param dbName database where the table lives
   * @param tableName table to drop
   * @throws HiveException thrown if the drop fails
   */
  public void dropTable(String dbName, String tableName) throws HiveException {
    dropTable(dbName, tableName, true, true);
  }  
  
  /**
   * Drops the table. 
   * @param tableName
   * @param deleteData deletes the underlying data along with metadata
   * @param ignoreUnknownTab an exception if thrown if this is falser and
   * table doesn't exist
   * @throws HiveException
   */
  public void dropTable(String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTab) throws HiveException {
    
    try {
      getMSC().dropTable(dbName, tableName, deleteData, ignoreUnknownTab);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    } 
  }

  public HiveConf getConf() {
    return (conf);
  }
  
  /**
   * Returns metadata of the table. 
   * @param dbName the name of the database
   * @param tableName the name of the table
   * @return the table
   * @exception HiveException if there's an internal error or if the 
   * table doesn't exist 
   */
  public Table getTable(final String dbName, final String tableName) 
    throws HiveException {
    
    return this.getTable(dbName, tableName, true);
  }  
  
  /**
   * Returns metadata of the table
   * @param dbName the name of the database
   * @param tableName the name of the table
   * @param throwException controls whether an exception is thrown 
   * or a returns a null
   * @return the table or if throwException is false a null value.
   * @throws HiveException
   */
  public Table getTable(final String dbName, final String tableName, 
      boolean throwException) throws HiveException {

    if(tableName == null || tableName.equals("")) {
      throw new HiveException("empty table creation??");
    }
    Table table = new Table();
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      tTable = getMSC().getTable(dbName, tableName);
    } catch (NoSuchObjectException e) {
      if(throwException) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidTableException("Table not found ", tableName);
      }
      return null;
    } catch (Exception e) {
      throw new HiveException("Unable to fetch table " + tableName, e);
    }
    // just a sanity check
    assert(tTable != null);
    try {
      
      // Use LazySimpleSerDe for MetadataTypedColumnsetSerDe.
      // NOTE: LazySimpleSerDe does not support tables with a single column of col
      // of type "array<string>".  This happens when the table is created using an
      // earlier version of Hive.
      if (tTable.getSd().getSerdeInfo().getSerializationLib().equals(
          org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class.getName())
          && tTable.getSd().getColsSize() > 0 
          && tTable.getSd().getCols().get(0).getType().indexOf('<') == -1 ) {
        tTable.getSd().getSerdeInfo().setSerializationLib(
            org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class.getName());
      }

      // first get a schema (in key / vals)
      Properties p = MetaStoreUtils.getSchema(tTable);
      table.setSchema(p);
      table.setTTable(tTable);
      table.setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>)
          Class.forName(table.getSchema().getProperty(org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
                                                      org.apache.hadoop.mapred.SequenceFileInputFormat.class.getName()),
                        true, JavaUtils.getClassLoader()));
      table.setOutputFormatClass((Class<? extends HiveOutputFormat>)
          Class.forName(table.getSchema().getProperty(org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
                                                      HiveSequenceFileOutputFormat.class.getName()),
                        true, JavaUtils.getClassLoader()));
      table.setDeserializer(MetaStoreUtils.getDeserializer(getConf(), p));
      table.setDataLocation(new URI(tTable.getSd().getLocation()));
    } catch(Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    String sf = table.getSerdeParam(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
    if(sf != null) {
      char[] b = sf.toCharArray();
      if ((b.length == 1) && (b[0] < 10)){ // ^A, ^B, ^C, ^D, \t
        table.setSerdeParam(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, Integer.toString(b[0]));
      }
    }
    table.checkValidity();
    return table;
  }
  
  public List<String> getAllTables() throws HiveException {
    return getTablesByPattern(".*");
  }
  
  /**
   * returns all existing tables that match the given pattern. The matching occurs as per Java regular expressions
   * @param tablePattern java re pattern
   * @return list of table names
   * @throws HiveException
   */
  public List<String> getTablesByPattern(String tablePattern) throws HiveException {
    try {
      return getMSC().getTables(MetaStoreUtils.DEFAULT_DATABASE_NAME, tablePattern);
    } catch(Exception e) {
      throw new HiveException(e);
    }
  }
  
  // for testing purposes
  protected List<String> getTablesForDb(String database, String tablePattern) throws HiveException {
    try {
      return getMSC().getTables(database, tablePattern);
    } catch(Exception e) {
      throw new HiveException(e);
    }
  }
  
  /**
   * @param name
   * @param locationUri
   * @return true or false
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#createDatabase(java.lang.String, java.lang.String)
   */
  protected boolean createDatabase(String name, String locationUri) throws AlreadyExistsException,
      MetaException, TException {
    return getMSC().createDatabase(name, locationUri);
  }

  /**
   * @param name
   * @return true or false
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.HiveMetaStoreClient#dropDatabase(java.lang.String)
   */
  protected boolean dropDatabase(String name) throws MetaException, TException {
    return getMSC().dropDatabase(name);
  }

  /**
   * Load a directory into a Hive Table Partition
   * - Alters existing content of the partition with the contents of loadPath.
   * - If he partition does not exist - one is created
   * - files in loadPath are moved into Hive. But the directory itself is not removed.
   *
   * @param loadPath Directory containing files to load into Table
   * @param tableName name of table to be loaded.
   * @param partSpec defines which partition needs to be loaded
   * @param replace if true - replace files in the partition, otherwise add files to the partition
   * @param tmpDirPath The temporary directory.
   */
  public void loadPartition(Path loadPath, String tableName,
      AbstractMap<String, String> partSpec, boolean replace,
      Path tmpDirPath)
  throws HiveException {
    Table tbl = getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    try {
      /** Move files before creating the partition since down stream processes check 
       *  for existence of partition in metadata before accessing the data. If partition
       *  is created before data is moved, downstream waiting processes might move forward
       *  with partial data
       */

      FileSystem fs;
      Path partPath;

      // check if partition exists without creating it
      Partition part = getPartition (tbl, partSpec, false);
      if (part == null) {
        // Partition does not exist currently. The partition name is extrapolated from
        // the table's location (even if the table is marked external)
        fs = FileSystem.get(tbl.getDataLocation(), getConf());
        partPath = new Path(tbl.getDataLocation().getPath(), Warehouse.makePartName(partSpec));
      } else {
        // Partition exists already. Get the path from the partition. This will
        // get the default path for Hive created partitions or the external path
        // when directly created by user
        partPath = part.getPath()[0];
        fs = partPath.getFileSystem(getConf());
      }
      if(replace) {
        Hive.replaceFiles(loadPath, partPath, fs, tmpDirPath);
      } else {
        Hive.copyFiles(loadPath, partPath, fs);
      }

      if (part == null) {
        // create the partition if it didn't exist before
        getPartition(tbl, partSpec, true);
      }
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    } catch (MetaException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
}

  /**
   * Load a directory into a Hive Table.
   * - Alters existing content of table with the contents of loadPath.
   * - If table does not exist - an exception is thrown
   * - files in loadPath are moved into Hive. But the directory itself is not removed.
   *
   * @param loadPath Directory containing files to load into Table
   * @param tableName name of table to be loaded.
   * @param replace if true - replace files in the table, otherwise add files to table
   * @param tmpDirPath The temporary directory.
   */
  public void loadTable(Path loadPath, String tableName, 
       boolean replace,
       Path tmpDirPath) throws HiveException {
    Table tbl = getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
    if(replace) {
      tbl.replaceFiles(loadPath, tmpDirPath);
    } else {
      tbl.copyFiles(loadPath);
    }
  }

  /**
   * Creates a partition.
   * @param tbl table for which partition needs to be created
   * @param partSpec partition keys and their values
   * @return created partition object
   * @throws HiveException if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, Map<String, String> partSpec)
    throws HiveException {
      return createPartition(tbl, partSpec, null);
  }
  
  /**
   * Creates a partition 
   * @param tbl table for which partition needs to be created
   * @param partSpec partition keys and their values
   * @param location location of this partition
   * @return created partition object
   * @throws HiveException if table doesn't exist or partition already exists
   */
  public Partition createPartition(Table tbl, Map<String, String> partSpec,
      Path location) throws HiveException {
        
    org.apache.hadoop.hive.metastore.api.Partition partition = null;
    
    try {
      Partition tmpPart = new Partition(tbl, partSpec, location);
      partition = getMSC().add_partition(tmpPart.getTPartition());
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    
    return new Partition(tbl, partition);
  }

  /**
   * Returns partition metadata
   * @param tbl the partition's table
   * @param partSpec partition keys and values
   * @param forceCreate if this is true and partition doesn't exist then a partition is created
   * @return result partition object or null if there is no partition
   * @throws HiveException
   */
  public Partition getPartition(Table tbl, Map<String, String> partSpec, boolean forceCreate)
      throws HiveException {
    if(!tbl.isValidSpec(partSpec)) {
      throw new HiveException("Invalid partition: " + partSpec);
    }
    List<String> pvals = new ArrayList<String>();
    for (FieldSchema field : tbl.getPartCols()) {
      String val = partSpec.get(field.getName());
      if(val == null || val.length() == 0) {
        throw new HiveException("Value for key " + field.getName() + " is null or empty");
      }
      pvals.add(val);
    }
    org.apache.hadoop.hive.metastore.api.Partition tpart = null;
    try {
      tpart = getMSC().getPartition(tbl.getDbName(), tbl.getName(), pvals);
      if(tpart == null && forceCreate) {
        LOG.debug("creating partition for table "  + tbl.getName() + " with partition spec : " + partSpec);
        tpart = getMSC().appendPartition(tbl.getDbName(), tbl.getName(), pvals);;
      }
      if(tpart == null){
        return null;
      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return new Partition(tbl, tpart);
  }
  
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      boolean deleteData) throws HiveException {
    try {
      return getMSC().dropPartition(db_name, tbl_name, part_vals, deleteData);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Partition or table doesn't exist.", e);
    } catch (Exception e) {
      throw new HiveException("Unknow error. Please check logs.", e);
    }
  }

  public List<String> getPartitionNames(String dbName, String tblName, short max) throws HiveException {
    List names = null;
    try {
      names = getMSC().listPartitionNames(dbName, tblName, max);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return names;
  }

  /**
   * get all the partitions that the table has
   * @param tbl object for which partition is needed
   * @return list of partition objects
   * @throws HiveException
   */
  public List<Partition> getPartitions(Table tbl) throws HiveException {
    if(tbl.isPartitioned()) {
      List<org.apache.hadoop.hive.metastore.api.Partition> tParts;
      try {
        tParts = getMSC().listPartitions(tbl.getDbName(), tbl.getName(), (short) -1);
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new HiveException(e);
      }
      List<Partition> parts = new ArrayList<Partition>(tParts.size());
      for (org.apache.hadoop.hive.metastore.api.Partition tpart : tParts) {
        parts.add(new Partition(tbl, tpart));
      }
      return parts;
    } else {
      // create an empty partition. 
      // HACK, HACK. SemanticAnalyzer code requires that an empty partition when the table is not partitioned
      org.apache.hadoop.hive.metastore.api.Partition tPart = new org.apache.hadoop.hive.metastore.api.Partition();
      tPart.setSd(tbl.getTTable().getSd()); // TODO: get a copy
      Partition part = new Partition(tbl, tPart);
      ArrayList<Partition> parts = new ArrayList<Partition>(1);
      parts.add(part);
      return parts;
    }
  }

  static private void checkPaths(FileSystem fs, FileStatus [] srcs, Path destf, boolean replace) throws HiveException {
    try {
        for(int i=0; i<srcs.length; i++) {
            FileStatus [] items = fs.listStatus(srcs[i].getPath());
            for(int j=0; j<items.length; j++) {

                if (Utilities.isTempPath(items[j])) {
                      // This check is redundant because temp files are removed by execution layer before
                      // calling loadTable/Partition. But leaving it in just in case.
                      fs.delete(items[j].getPath(), true);
                      continue;
                }
                if(items[j].isDir()) {
                    throw new HiveException("checkPaths: "+srcs[i].toString()+" has nested directory"+
                                            items[j].toString());
                }
                Path tmpDest = new Path(destf, items[j].getPath().getName());
                if(!replace && fs.exists(tmpDest)) {
                    throw new HiveException("checkPaths: " + tmpDest + " already exists");
                }
            }
        }
    } catch (IOException e) {
        throw new HiveException("checkPaths: filesystem error in check phase", e);
    }
}

  static protected void copyFiles(Path srcf, Path destf, FileSystem fs) throws HiveException {
    FileStatus[] srcs;
    try {
      srcs = fs.globStatus(srcf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      return;
      //srcs = new FileStatus[0]; Why is this needed?
    }
    // check that source and target paths exist
    checkPaths(fs, srcs, destf, false);

    // move it, move it
    try {
      for(int i=0; i<srcs.length; i++) {
        FileStatus [] items = fs.listStatus(srcs[i].getPath());
        for(int j=0; j<items.length; j++) {
          fs.rename(items[j].getPath(), new Path(destf, items[j].getPath().getName()));
        }
      }
    } catch (IOException e) {
      throw new HiveException("addFiles: error while moving files!!!", e);
    }
  }

  /**
   * Replaces files in the partition with new data set specifed by srcf. Works by moving files
   *
   * @param srcf Files to be moved. Leaf Directories or Globbed File Paths
   * @param destf The directory where the final data needs to go
   * @param fs The filesystem handle
   * @param tmppath Temporary directory
   */
  static protected void replaceFiles(Path srcf, Path destf, FileSystem fs,
      Path tmppath) throws HiveException {
      FileStatus [] srcs;
      try {
          srcs = fs.globStatus(srcf);
      } catch (IOException e) {
          throw new HiveException("addFiles: filesystem error in check phase", e);
      }
      if (srcs == null) {
        LOG.info("No sources specified to move: " + srcf);
        return;
        //srcs = new FileStatus[0]; Why is this needed?
      }
      checkPaths(fs, srcs, destf, true);

      try {
          fs.mkdirs(tmppath);
          for(int i=0; i<srcs.length; i++) {
              FileStatus[] items = fs.listStatus(srcs[i].getPath());
              for(int j=0; j<items.length; j++) {
                if (!fs.rename(items[j].getPath(),
                               new Path(tmppath, items[j].getPath().getName()))) {
                  throw new HiveException ("Error moving: " + items[j].getPath()
                                           + " into: " + tmppath);
                }
              }
          }

          // point of no return
          boolean b = fs.delete(destf, true);
          LOG.debug("Deleting:"+destf.toString()+",Status:"+b);

          // create the parent directory otherwise rename can fail if the parent doesn't exist
          if (!fs.mkdirs(destf.getParent())) {
            throw new HiveException("Unable to create destination directory: " 
                  + destf.getParent().toString());
          }
          
          b = fs.rename(tmppath, destf);
          if (!b) {
            throw new HiveException("Unable to move results to destination directory: " 
                + destf.getParent().toString());
          }
          LOG.debug("Renaming:"+tmppath.toString()+",Status:"+b);

      } catch (IOException e) {
          throw new HiveException("replaceFiles: error while moving files!!!", e);
      } finally {
          try {
              fs.delete(tmppath, true);
          } catch (IOException e) {
            LOG.warn("Unable delete path " + tmppath, e);
          }
      }
  }

  /**
   * Creates a metastore client. Currently it creates only JDBC based client as File based store
   * support is removed
   * @returns a Meta Store Client
   * @throws HiveMetaException  if a working client can't be created
   */
  private IMetaStoreClient createMetaStoreClient() throws MetaException {
      return new HiveMetaStoreClient(this.conf);
  }
  
  /**
   * 
   * @return the metastore client for the current thread
   * @throws MetaException
   */
  private IMetaStoreClient getMSC() throws MetaException {
    IMetaStoreClient msc = threadLocalMSC.get();
    if(msc == null) {
      msc = this.createMetaStoreClient();
    }
    return msc;
  }

  public static List<FieldSchema> getFieldsFromDeserializer(String name, Deserializer serde) throws HiveException {
    try {
      return MetaStoreUtils.getFieldsFromDeserializer(name, serde);
    } catch (SerDeException e) {
      throw new HiveException("Error in getting fields from serde. " + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Error in getting fields from serde." + e.getMessage(), e);
    }
  }
};
