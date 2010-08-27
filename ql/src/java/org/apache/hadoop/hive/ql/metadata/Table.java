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
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
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
  private URI uri;
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
      sd.getSerdeInfo().getParameters().put(Constants.SERIALIZATION_FORMAT, "1");
      sd.setInputFormat(SequenceFileInputFormat.class.getName());
      sd.setOutputFormat(HiveSequenceFileOutputFormat.class.getName());
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
      if (null == getDeserializer()) {
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

  final public Properties getSchema() {
    return MetaStoreUtils.getSchema(tTable);
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

  final public URI getDataLocation() {
    if (uri == null) {
      Path path = getPath();
      if (path != null) {
        uri = path.toUri();
      }
    }
    return uri;
  }

  final public Deserializer getDeserializer() {
    if (deserializer == null) {
      try {
        deserializer = MetaStoreUtils.getDeserializer(Hive.get().getConf(), tTable);
      } catch (MetaException e) {
        throw new RuntimeException(e);
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
    }
    return deserializer;
  }

  public HiveStorageHandler getStorageHandler() {
    if (storageHandler != null) {
      return storageHandler;
    }
    try {
      storageHandler = HiveUtils.getStorageHandler(
        Hive.get().getConf(),
        getProperty(
          org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_STORAGE));
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
          c = Class.forName(className, true,
            JavaUtils.getClassLoader());
        }
        if (!HiveOutputFormat.class.isAssignableFrom(c)) {
          outputFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(c);
        } else {
          outputFormatClass = (Class<? extends HiveOutputFormat>)c;
        }
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    return outputFormatClass;
  }

  final public boolean isValidSpec(Map<String, String> spec)
      throws HiveException {

    // TODO - types need to be checked.
    List<FieldSchema> partCols = tTable.getPartitionKeys();
    if (partCols == null || (partCols.size() == 0)) {
      if (spec != null) {
        throw new HiveException(
            "table is not partitioned but partition spec exists: " + spec);
      } else {
        return true;
      }
    }

    if ((spec == null) || (spec.size() != partCols.size())) {
      throw new HiveException(
          "table is partitioned but partition spec is not specified or tab: "
              + spec);
    }

    for (FieldSchema field : partCols) {
      if (spec.get(field.getName()) == null) {
        throw new HiveException(field.getName()
            + " not found in table's partition spec: " + spec);
      }
    }

    return true;
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

  public void setDataLocation(URI uri) {
    this.uri = uri;
    tTable.getSd().setLocation(uri.toString());
  }

  public void unsetDataLocation() {
    this.uri = null;
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
   * Replaces files in the partition with new data set specified by srcf. Works
   * by moving files
   *
   * @param srcf
   *          Files to be replaced. Leaf directories or globbed file paths
   * @param tmpd
   *          Temporary directory
   */
  protected void replaceFiles(Path srcf, Path tmpd) throws HiveException {
    FileSystem fs;
    try {
      fs = FileSystem.get(getDataLocation(), Hive.get().getConf());
      Hive.replaceFiles(srcf, new Path(getDataLocation().getPath()), fs, tmpd);
    } catch (IOException e) {
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
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
      fs = FileSystem.get(getDataLocation(), Hive.get().getConf());
      Hive.copyFiles(srcf, new Path(getDataLocation().getPath()), fs);
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
          .getOutputFormatSubstitute(origin));
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
    return new Table(tTable.clone());
  }

  public void setCreateTime(int createTime) {
    tTable.setCreateTime(createTime);
  }

  public boolean isNonNative() {
    return getProperty(
      org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_STORAGE)
      != null;
  }

  /**
   * @param protectMode
   */
  public void setProtectMode(ProtectMode protectMode){
    Map<String, String> parameters = tTable.getParameters();
    parameters.put(ProtectMode.PARAMETER_NAME, protectMode.toString());
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
    return (!mode.noDrop && !mode.offline && !mode.readOnly);
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
};
