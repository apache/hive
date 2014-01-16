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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HivePassThroughOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

/**
 * A Hive Table: is a fundamental unit of data in Hive that shares a common schema/DDL.
 *
 * Please note that the ql code should always go through methods of this class to access the
 * metadata, instead of directly accessing org.apache.hadoop.hive.metastore.api.Table.  This
 * helps to isolate the metastore code and the ql code.
 */
public class Table implements Serializable {

  private static final long serialVersionUID = 1L;

  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Table");

  private org.apache.hadoop.hive.metastore.api.Table tTable;

  /**
   * These fields are all cached fields.  The information comes from tTable.
   */
  private Deserializer deserializer;
  private Class<? extends HiveOutputFormat> outputFormatClass;
  private Class<? extends InputFormat> inputFormatClass;
  private Path path;
  private HiveStorageHandler storageHandler;

  /**
   * Used only for serialization.
   */
  public Table() {
  }

  public Table(org.apache.hadoop.hive.metastore.api.Table table) {
    tTable = table;
    if (!isView()) {
      // This will set up field: inputFormatClass
      getInputFormatClass();
      // This will set up field: outputFormatClass
      getOutputFormatClass();
    }
  }

  public Table(String databaseName, String tableName) {
    this(getEmptyTable(databaseName, tableName));
  }

  /**
   * This function should only be used in serialization.
   * We should never call this function to modify the fields, because
   * the cached fields will become outdated.
   */
  public org.apache.hadoop.hive.metastore.api.Table getTTable() {
    return tTable;
  }

  /**
   * This function should only be called by Java serialization.
   */
  public void setTTable(org.apache.hadoop.hive.metastore.api.Table tTable) {
    this.tTable = tTable;
  }

  /**
   * Initialize an emtpy table.
   */
  static org.apache.hadoop.hive.metastore.api.Table
  getEmptyTable(String databaseName, String tableName) {
    StorageDescriptor sd = new StorageDescriptor();
    {
      sd.setSerdeInfo(new SerDeInfo());
      sd.setNumBuckets(-1);
      sd.setBucketCols(new ArrayList<String>());
      sd.setCols(new ArrayList<FieldSchema>());
      sd.setParameters(new HashMap<String, String>());
      sd.setSortCols(new ArrayList<Order>());
      sd.getSerdeInfo().setParameters(new HashMap<String, String>());
      // We have to use MetadataTypedColumnsetSerDe because LazySimpleSerDe does
      // not support a table with no columns.
      sd.getSerdeInfo().setSerializationLib(MetadataTypedColumnsetSerDe.class.getName());
      sd.getSerdeInfo().getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1");
      sd.setInputFormat(SequenceFileInputFormat.class.getName());
      sd.setOutputFormat(HiveSequenceFileOutputFormat.class.getName());
      SkewedInfo skewInfo = new SkewedInfo();
      skewInfo.setSkewedColNames(new ArrayList<String>());
      skewInfo.setSkewedColValues(new ArrayList<List<String>>());
      skewInfo.setSkewedColValueLocationMaps(new HashMap<List<String>, String>());
      sd.setSkewedInfo(skewInfo);
    }

    org.apache.hadoop.hive.metastore.api.Table t = new org.apache.hadoop.hive.metastore.api.Table();
    {
      t.setSd(sd);
      t.setPartitionKeys(new ArrayList<FieldSchema>());
      t.setParameters(new HashMap<String, String>());
      t.setTableType(TableType.MANAGED_TABLE.toString());
      t.setDbName(databaseName);
      t.setTableName(tableName);
    }
    return t;
  }

  public void checkValidity() throws HiveException {
    // check for validity
    String name = tTable.getTableName();
    if (null == name || name.length() == 0
        || !MetaStoreUtils.validateName(name)) {
      throw new HiveException("[" + name + "]: is not a valid table name");
    }
    if (0 == getCols().size()) {
      throw new HiveException(
          "at least one column must be specified for the table");
    }
    if (!isView()) {
      if (null == getDeserializerFromMetaStore()) {
        throw new HiveException("must specify a non-null serDe");
      }
      if (null == getInputFormatClass()) {
        throw new HiveException("must specify an InputFormat class");
      }
      if (null == getOutputFormatClass()) {
        throw new HiveException("must specify an OutputFormat class");
      }
    }

    if (isView()) {
      assert(getViewOriginalText() != null);
      assert(getViewExpandedText() != null);
    } else {
      assert(getViewOriginalText() == null);
      assert(getViewExpandedText() == null);
    }

    Iterator<FieldSchema> iterCols = getCols().iterator();
    List<String> colNames = new ArrayList<String>();
    while (iterCols.hasNext()) {
      String colName = iterCols.next().getName();
      if (!MetaStoreUtils.validateColumnName(colName)) {
        throw new HiveException("Invalid column name '" + colName
            + "' in the table definition");
      }
      Iterator<String> iter = colNames.iterator();
      while (iter.hasNext()) {
        String oldColName = iter.next();
        if (colName.equalsIgnoreCase(oldColName)) {
          throw new HiveException("Duplicate column name " + colName
              + " in the table definition.");
        }
      }
      colNames.add(colName.toLowerCase());
    }

    if (getPartCols() != null) {
      // there is no overlap between columns and partitioning columns
      Iterator<FieldSchema> partColsIter = getPartCols().iterator();
      while (partColsIter.hasNext()) {
        String partCol = partColsIter.next().getName();
        if (colNames.contains(partCol.toLowerCase())) {
          throw new HiveException("Partition column name " + partCol
              + " conflicts with table columns.");
        }
      }
    }
    return;
  }

  public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    tTable.getSd().setInputFormat(inputFormatClass.getName());
  }

  public void setOutputFormatClass(Class<? extends HiveOutputFormat> outputFormatClass) {
    this.outputFormatClass = outputFormatClass;
    tTable.getSd().setOutputFormat(outputFormatClass.getName());
  }

  final public Properties getMetadata() {
    return MetaStoreUtils.getTableMetadata(tTable);
  }

  final public Path getPath() {
    String location = tTable.getSd().getLocation();
    if (location == null) {
      return null;
    }
    return new Path(location);
  }

  final public String getTableName() {
    return tTable.getTableName();
  }

  final public Path getDataLocation() {
    if (path == null) {
      path = getPath();
    }
    return path;
  }

  final public Deserializer getDeserializer() {
    if (deserializer == null) {
      deserializer = getDeserializerFromMetaStore();
    }
    return deserializer;
  }

  private Deserializer getDeserializerFromMetaStore() {
    try {
      return MetaStoreUtils.getDeserializer(Hive.get().getConf(), tTable);
    } catch (MetaException e) {
      throw new RuntimeException(e);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  public HiveStorageHandler getStorageHandler() {
    if (storageHandler != null) {
      return storageHandler;
    }
    try {
      storageHandler = HiveUtils.getStorageHandler(
        Hive.get().getConf(),
        getProperty(
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return storageHandler;
  }

  final public Class<? extends InputFormat> getInputFormatClass() {
    if (inputFormatClass == null) {
      try {
        String className = tTable.getSd().getInputFormat();
        if (className == null) {
          if (getStorageHandler() == null) {
            return null;
          }
          inputFormatClass = getStorageHandler().getInputFormatClass();
        } else {
          inputFormatClass = (Class<? extends InputFormat>)
            Class.forName(className, true, JavaUtils.getClassLoader());
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return inputFormatClass;
  }

  final public Class<? extends HiveOutputFormat> getOutputFormatClass() {
    // Replace FileOutputFormat for backward compatibility
    boolean storagehandler = false;
    if (outputFormatClass == null) {
      try {
        String className = tTable.getSd().getOutputFormat();
        Class<?> c;
        if (className == null) {
          if (getStorageHandler() == null) {
            return null;
          }
          c = getStorageHandler().getOutputFormatClass();
        } else {
            // if HivePassThroughOutputFormat
            if (className.equals(
                 HivePassThroughOutputFormat.HIVE_PASSTHROUGH_OF_CLASSNAME)) {
              if (getStorageHandler() != null) {
                // get the storage handler real output format class
                c = getStorageHandler().getOutputFormatClass();
              }
              else {
                //should not happen
                return null;
              }
            }
            else {
              c = Class.forName(className, true,
                  JavaUtils.getClassLoader());
            }
        }
        if (!HiveOutputFormat.class.isAssignableFrom(c)) {
          if (getStorageHandler() != null) {
            storagehandler = true;
          }
          else {
            storagehandler = false;
          }
          outputFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(c,storagehandler);
        } else {
          outputFormatClass = (Class<? extends HiveOutputFormat>)c;
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return outputFormatClass;
  }

  final public void validatePartColumnNames(
      Map<String, String> spec, boolean shouldBeFull) throws SemanticException {
    List<FieldSchema> partCols = tTable.getPartitionKeys();
    if (partCols == null || (partCols.size() == 0)) {
      if (spec != null) {
        throw new SemanticException("table is not partitioned but partition spec exists: " + spec);
      }
      return;
    } else if (spec == null) {
      if (shouldBeFull) {
        throw new SemanticException("table is partitioned but partition spec is not specified");
      }
      return;
    }
    int columnsFound = 0;
    for (FieldSchema fs : partCols) {
      if (spec.containsKey(fs.getName())) {
        ++columnsFound;
      }
      if (columnsFound == spec.size()) break;
    }
    if (columnsFound < spec.size()) {
      throw new SemanticException("Partition spec " + spec + " contains non-partition columns");
    }
    if (shouldBeFull && (spec.size() != partCols.size())) {
      throw new SemanticException("partition spec " + spec
          + " doesn't contain all (" + partCols.size() + ") partition columns");
    }
  }

  public void setProperty(String name, String value) {
    tTable.getParameters().put(name, value);
  }

  public String getProperty(String name) {
    return tTable.getParameters().get(name);
  }

  public void setTableType(TableType tableType) {
     tTable.setTableType(tableType.toString());
   }

  public TableType getTableType() {
     return Enum.valueOf(TableType.class, tTable.getTableType());
   }

  public ArrayList<StructField> getFields() {

    ArrayList<StructField> fields = new ArrayList<StructField>();
    try {
      Deserializer decoder = getDeserializer();

      // Expand out all the columns of the table
      StructObjectInspector structObjectInspector = (StructObjectInspector) decoder
          .getObjectInspector();
      List<? extends StructField> fld_lst = structObjectInspector
          .getAllStructFieldRefs();
      for (StructField field : fld_lst) {
        fields.add(field);
      }
    } catch (SerDeException e) {
      throw new RuntimeException(e);
    }
    return fields;
  }

  public StructField getField(String fld) {
    try {
      StructObjectInspector structObjectInspector = (StructObjectInspector) getDeserializer()
          .getObjectInspector();
      return structObjectInspector.getStructFieldRef(fld);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

   @Override
  public String toString() {
    return tTable.getTableName();
  }

   /* (non-Javadoc)
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode() {
     final int prime = 31;
     int result = 1;
     result = prime * result + ((tTable == null) ? 0 : tTable.hashCode());
     return result;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#equals(java.lang.Object)
    */
   @Override
   public boolean equals(Object obj) {
     if (this == obj) {
       return true;
     }
     if (obj == null) {
       return false;
     }
     if (getClass() != obj.getClass()) {
       return false;
     }
     Table other = (Table) obj;
     if (tTable == null) {
       if (other.tTable != null) {
         return false;
       }
     } else if (!tTable.equals(other.tTable)) {
       return false;
     }
     return true;
   }


  public List<FieldSchema> getPartCols() {
    List<FieldSchema> partKeys = tTable.getPartitionKeys();
    if (partKeys == null) {
      partKeys = new ArrayList<FieldSchema>();
      tTable.setPartitionKeys(partKeys);
    }
    return partKeys;
  }

  public boolean isPartitionKey(String colName) {
    for (FieldSchema key : getPartCols()) {
      if (key.getName().toLowerCase().equals(colName)) {
        return true;
      }
    }
    return false;
  }

  // TODO merge this with getBucketCols function
  public String getBucketingDimensionId() {
    List<String> bcols = tTable.getSd().getBucketCols();
    if (bcols == null || bcols.size() == 0) {
      return null;
    }

    if (bcols.size() > 1) {
      LOG.warn(this
          + " table has more than one dimensions which aren't supported yet");
    }

    return bcols.get(0);
  }

  public void setDataLocation(Path path) {
    this.path = path;
    tTable.getSd().setLocation(path.toString());
  }

  public void unsetDataLocation() {
    this.path = null;
    tTable.getSd().unsetLocation();
  }

  public void setBucketCols(List<String> bucketCols) throws HiveException {
    if (bucketCols == null) {
      return;
    }

    for (String col : bucketCols) {
      if (!isField(col)) {
        throw new HiveException("Bucket columns " + col
            + " is not part of the table columns (" + getCols() );
      }
    }
    tTable.getSd().setBucketCols(bucketCols);
  }

  public void setSortCols(List<Order> sortOrder) throws HiveException {
    tTable.getSd().setSortCols(sortOrder);
  }

  public void setSkewedValueLocationMap(List<String> valList, String dirName)
      throws HiveException {
    Map<List<String>, String> mappings = tTable.getSd().getSkewedInfo()
        .getSkewedColValueLocationMaps();
    if (null == mappings) {
      mappings = new HashMap<List<String>, String>();
      tTable.getSd().getSkewedInfo().setSkewedColValueLocationMaps(mappings);
    }

    // Add or update new mapping
    mappings.put(valList, dirName);
  }

  public Map<List<String>,String> getSkewedColValueLocationMaps() {
    return (tTable.getSd().getSkewedInfo() != null) ? tTable.getSd().getSkewedInfo()
        .getSkewedColValueLocationMaps() : new HashMap<List<String>, String>();
  }

  public void setSkewedColValues(List<List<String>> skewedValues) throws HiveException {
    tTable.getSd().getSkewedInfo().setSkewedColValues(skewedValues);
  }

  public List<List<String>> getSkewedColValues(){
    return (tTable.getSd().getSkewedInfo() != null) ? tTable.getSd().getSkewedInfo()
        .getSkewedColValues() : new ArrayList<List<String>>();
  }

  public void setSkewedColNames(List<String> skewedColNames) throws HiveException {
    tTable.getSd().getSkewedInfo().setSkewedColNames(skewedColNames);
  }

  public List<String> getSkewedColNames() {
    return (tTable.getSd().getSkewedInfo() != null) ? tTable.getSd().getSkewedInfo()
        .getSkewedColNames() : new ArrayList<String>();
  }

  public SkewedInfo getSkewedInfo() {
    return tTable.getSd().getSkewedInfo();
  }

  public void setSkewedInfo(SkewedInfo skewedInfo) throws HiveException {
    tTable.getSd().setSkewedInfo(skewedInfo);
  }

  public boolean isStoredAsSubDirectories() {
    return tTable.getSd().isStoredAsSubDirectories();
  }

  public void setStoredAsSubDirectories(boolean storedAsSubDirectories) throws HiveException {
    tTable.getSd().setStoredAsSubDirectories(storedAsSubDirectories);
  }

  private boolean isField(String col) {
    for (FieldSchema field : getCols()) {
      if (field.getName().equals(col)) {
        return true;
      }
    }
    return false;
  }

  public List<FieldSchema> getCols() {
    boolean getColsFromSerDe = SerDeUtils.shouldGetColsFromSerDe(
      getSerializationLib());
    if (!getColsFromSerDe) {
      return tTable.getSd().getCols();
    } else {
      try {
        return Hive.getFieldsFromDeserializer(getTableName(), getDeserializer());
      } catch (HiveException e) {
        LOG.error("Unable to get field from serde: " + getSerializationLib(), e);
      }
      return new ArrayList<FieldSchema>();
    }
  }

  /**
   * Returns a list of all the columns of the table (data columns + partition
   * columns in that order.
   *
   * @return List<FieldSchema>
   */
  public List<FieldSchema> getAllCols() {
    ArrayList<FieldSchema> f_list = new ArrayList<FieldSchema>();
    f_list.addAll(getPartCols());
    f_list.addAll(getCols());
    return f_list;
  }

  public void setPartCols(List<FieldSchema> partCols) {
    tTable.setPartitionKeys(partCols);
  }

  public String getDbName() {
    return tTable.getDbName();
  }

  public int getNumBuckets() {
    return tTable.getSd().getNumBuckets();
  }

  /**
   * Replaces the directory corresponding to the table by srcf. Works by
   * deleting the table directory and renaming the source directory.
   *
   * @param srcf
   *          Source directory
   */
  protected void replaceFiles(Path srcf) throws HiveException {
    Path tableDest = getPath();
    Hive.replaceFiles(srcf, tableDest, tableDest, Hive.get().getConf());
  }

  /**
   * Inserts files specified into the partition. Works by moving files
   *
   * @param srcf
   *          Files to be moved. Leaf directories or globbed file paths
   */
  protected void copyFiles(Path srcf) throws HiveException {
    FileSystem fs;
    try {
      fs = getDataLocation().getFileSystem(Hive.get().getConf());
      Hive.copyFiles(Hive.get().getConf(), srcf, getPath(), fs);
    } catch (IOException e) {
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
  }

  public void setInputFormatClass(String name) throws HiveException {
    if (name == null) {
      inputFormatClass = null;
      tTable.getSd().setInputFormat(null);
      return;
    }
    try {
      setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>) Class
          .forName(name, true, JavaUtils.getClassLoader()));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }

  public void setOutputFormatClass(String name) throws HiveException {
    if (name == null) {
      outputFormatClass = null;
      tTable.getSd().setOutputFormat(null);
      return;
    }
    try {
      Class<?> origin = Class.forName(name, true, JavaUtils.getClassLoader());
      setOutputFormatClass(HiveFileFormatUtils
          .getOutputFormatSubstitute(origin,false));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }

  public boolean isPartitioned() {
    if (getPartCols() == null) {
      return false;
    }
    return (getPartCols().size() != 0);
  }

  public void setFields(List<FieldSchema> fields) {
    tTable.getSd().setCols(fields);
  }

  public void setNumBuckets(int nb) {
    tTable.getSd().setNumBuckets(nb);
  }

  /**
   * @return The owner of the table.
   * @see org.apache.hadoop.hive.metastore.api.Table#getOwner()
   */
  public String getOwner() {
    return tTable.getOwner();
  }

  /**
   * @return The table parameters.
   * @see org.apache.hadoop.hive.metastore.api.Table#getParameters()
   */
  public Map<String, String> getParameters() {
    return tTable.getParameters();
  }

  /**
   * @return The retention on the table.
   * @see org.apache.hadoop.hive.metastore.api.Table#getRetention()
   */
  public int getRetention() {
    return tTable.getRetention();
  }

  /**
   * @param owner
   * @see org.apache.hadoop.hive.metastore.api.Table#setOwner(java.lang.String)
   */
  public void setOwner(String owner) {
    tTable.setOwner(owner);
  }

  /**
   * @param retention
   * @see org.apache.hadoop.hive.metastore.api.Table#setRetention(int)
   */
  public void setRetention(int retention) {
    tTable.setRetention(retention);
  }

  private SerDeInfo getSerdeInfo() {
    return tTable.getSd().getSerdeInfo();
  }

  public void setSerializationLib(String lib) {
    getSerdeInfo().setSerializationLib(lib);
  }

  public String getSerializationLib() {
    return getSerdeInfo().getSerializationLib();
  }

  public String getSerdeParam(String param) {
    return getSerdeInfo().getParameters().get(param);
  }

  public String setSerdeParam(String param, String value) {
    return getSerdeInfo().getParameters().put(param, value);
  }

  public List<String> getBucketCols() {
    return tTable.getSd().getBucketCols();
  }

  public List<Order> getSortCols() {
    return tTable.getSd().getSortCols();
  }

  public void setTableName(String tableName) {
    tTable.setTableName(tableName);
  }

  public void setDbName(String databaseName) {
    tTable.setDbName(databaseName);
  }

  public List<FieldSchema> getPartitionKeys() {
    return tTable.getPartitionKeys();
  }

  /**
   * @return the original view text, or null if this table is not a view
   */
  public String getViewOriginalText() {
    return tTable.getViewOriginalText();
  }

  /**
   * @param viewOriginalText
   *          the original view text to set
   */
  public void setViewOriginalText(String viewOriginalText) {
    tTable.setViewOriginalText(viewOriginalText);
  }

  /**
   * @return the expanded view text, or null if this table is not a view
   */
  public String getViewExpandedText() {
    return tTable.getViewExpandedText();
  }

  public void clearSerDeInfo() {
    tTable.getSd().getSerdeInfo().getParameters().clear();
  }
  /**
   * @param viewExpandedText
   *          the expanded view text to set
   */
  public void setViewExpandedText(String viewExpandedText) {
    tTable.setViewExpandedText(viewExpandedText);
  }

  /**
   * @return whether this table is actually a view
   */
  public boolean isView() {
    return TableType.VIRTUAL_VIEW.equals(getTableType());
  }

  /**
   * @return whether this table is actually an index table
   */
  public boolean isIndexTable() {
    return TableType.INDEX_TABLE.equals(getTableType());
  }

  /**
   * Creates a partition name -> value spec map object
   *
   * @param tp
   *          Use the information from this partition.
   * @return Partition name to value mapping.
   */
  public LinkedHashMap<String, String> createSpec(
      org.apache.hadoop.hive.metastore.api.Partition tp) {

    List<FieldSchema> fsl = getPartCols();
    List<String> tpl = tp.getValues();
    LinkedHashMap<String, String> spec = new LinkedHashMap<String, String>();
    for (int i = 0; i < fsl.size(); i++) {
      FieldSchema fs = fsl.get(i);
      String value = tpl.get(i);
      spec.put(fs.getName(), value);
    }
    return spec;
  }

  public Table copy() throws HiveException {
    return new Table(tTable.deepCopy());
  }

  public void setCreateTime(int createTime) {
    tTable.setCreateTime(createTime);
  }

  public int getLastAccessTime() {
    return tTable.getLastAccessTime();
  }

  public void setLastAccessTime(int lastAccessTime) {
    tTable.setLastAccessTime(lastAccessTime);
  }

  public boolean isNonNative() {
    return getProperty(
      org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE)
      != null;
  }

  /**
   * @param protectMode
   */
  public void setProtectMode(ProtectMode protectMode){
    Map<String, String> parameters = tTable.getParameters();
    String pm = protectMode.toString();
    if (pm != null) {
      parameters.put(ProtectMode.PARAMETER_NAME, pm);
    } else {
      parameters.remove(ProtectMode.PARAMETER_NAME);
    }
    tTable.setParameters(parameters);
  }

  /**
   * @return protect mode
   */
  public ProtectMode getProtectMode(){
    Map<String, String> parameters = tTable.getParameters();

    if (!parameters.containsKey(ProtectMode.PARAMETER_NAME)) {
      return new ProtectMode();
    } else {
      return ProtectMode.getProtectModeFromString(
          parameters.get(ProtectMode.PARAMETER_NAME));
    }
  }

  /**
   * @return True protect mode indicates the table if offline.
   */
  public boolean isOffline(){
    return getProtectMode().offline;
  }

  /**
   * @return True if protect mode attribute of the partition indicate
   * that it is OK to drop the partition
   */
  public boolean canDrop() {
    ProtectMode mode = getProtectMode();
    return (!mode.noDrop && !mode.offline && !mode.readOnly && !mode.noDropCascade);
  }

  /**
   * @return True if protect mode attribute of the table indicate
   * that it is OK to write the table
   */
  public boolean canWrite() {
    ProtectMode mode = getProtectMode();
    return (!mode.offline && !mode.readOnly);
  }

  /**
   * @return include the db name
   */
  public String getCompleteName() {
    return getDbName() + "@" + getTableName();
  }

  /**
   * @return List containing Indexes names if there are indexes on this table
   * @throws HiveException
   **/
  public List<Index> getAllIndexes(short max) throws HiveException {
    Hive hive = Hive.get();
    return hive.getIndexes(getTTable().getDbName(), getTTable().getTableName(), max);
  }

  @SuppressWarnings("nls")
  public FileStatus[] getSortedPaths() {
    try {
      // Previously, this got the filesystem of the Table, which could be
      // different from the filesystem of the partition.
      FileSystem fs = FileSystem.get(getPath().toUri(), Hive.get()
          .getConf());
      String pathPattern = getPath().toString();
      if (getNumBuckets() > 0) {
        pathPattern = pathPattern + "/*";
      }
      LOG.info("Path pattern = " + pathPattern);
      FileStatus srcs[] = fs.globStatus(new Path(pathPattern));
      Arrays.sort(srcs);
      for (FileStatus src : srcs) {
        LOG.info("Got file: " + src.getPath());
      }
      if (srcs.length == 0) {
        return null;
      }
      return srcs;
    } catch (Exception e) {
      throw new RuntimeException("Cannot get path ", e);
    }
  }
};
