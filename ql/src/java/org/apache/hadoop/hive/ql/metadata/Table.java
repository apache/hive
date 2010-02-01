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
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
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

/**
 * A Hive Table: is a fundamental unit of data in Hive that shares a common
 * schema/DDL
 */
public class Table {

  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Table");

  private Properties schema;
  private Deserializer deserializer;
  private URI uri;
  private Class<? extends InputFormat> inputFormatClass;
  private Class<? extends HiveOutputFormat> outputFormatClass;
  private org.apache.hadoop.hive.metastore.api.Table tTable;

  /**
   * Table (only used internally)
   * 
   * @throws HiveException
   * 
   */
  protected Table() throws HiveException {
  }

  /**
   * Table
   * 
   * Create a TableMetaInfo object presumably with the intent of saving it to
   * the metastore
   * 
   * @param name
   *          the name of this table in the metadb
   * @param schema
   *          an object that represents the schema that this SerDe must know
   * @param deserializer
   *          a Class to be used for deserializing the data
   * @param dataLocation
   *          where is the table ? (e.g.,
   *          dfs://hadoop001.sf2p.facebook.com:9000/
   *          user/facebook/warehouse/example) NOTE: should not be hardcoding
   *          this, but ok for now
   * 
   * @exception HiveException
   *              on internal error. Note not possible now, but in the future
   *              reserve the right to throw an exception
   */
  public Table(String name, Properties schema, Deserializer deserializer,
      Class<? extends InputFormat<?, ?>> inputFormatClass,
      Class<?> outputFormatClass, URI dataLocation, Hive hive)
      throws HiveException {
    initEmpty();
    this.schema = schema;
    this.deserializer = deserializer; // TODO: convert to SerDeInfo format
    getTTable().getSd().getSerdeInfo().setSerializationLib(
        deserializer.getClass().getName());
    getTTable().setTableName(name);
    getSerdeInfo().setSerializationLib(deserializer.getClass().getName());
    setInputFormatClass(inputFormatClass);
    setOutputFormatClass(HiveFileFormatUtils
        .getOutputFormatSubstitute(outputFormatClass));
    setDataLocation(dataLocation);
  }

  public Table(String name) {
    // fill in defaults
    initEmpty();
    getTTable().setTableName(name);
    getTTable().setDbName(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    // We have to use MetadataTypedColumnsetSerDe because LazySimpleSerDe does
    // not
    // support a table with no columns.
    getSerdeInfo().setSerializationLib(
        MetadataTypedColumnsetSerDe.class.getName());
    getSerdeInfo().getParameters().put(Constants.SERIALIZATION_FORMAT, "1");
  }

  void initEmpty() {
    setTTable(new org.apache.hadoop.hive.metastore.api.Table());
    getTTable().setSd(new StorageDescriptor());
    getTTable().setPartitionKeys(new ArrayList<FieldSchema>());
    getTTable().setParameters(new HashMap<String, String>());

    StorageDescriptor sd = getTTable().getSd();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<String>());
    sd.setCols(new ArrayList<FieldSchema>());
    sd.setParameters(new HashMap<String, String>());
    sd.setSortCols(new ArrayList<Order>());

    sd.getSerdeInfo().setParameters(new HashMap<String, String>());

    setTableType(TableType.MANAGED_TABLE);
  }

  public void reinitSerDe() throws HiveException {
    try {
      deserializer = MetaStoreUtils.getDeserializer(Hive.get().getConf(),
          getTTable());
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  protected void initSerDe() throws HiveException {
    if (deserializer == null) {
      try {
        deserializer = MetaStoreUtils.getDeserializer(Hive.get().getConf(),
            getTTable());
      } catch (MetaException e) {
        throw new HiveException(e);
      }
    }
  }

  public void checkValidity() throws HiveException {
    // check for validity
    String name = getTTable().getTableName();
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

  /**
   * @param inputFormatClass
   */
  public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    tTable.getSd().setInputFormat(inputFormatClass.getName());
  }

  /**
   * @param class1
   */
  public void setOutputFormatClass(Class<?> class1) {
    outputFormatClass = HiveFileFormatUtils.getOutputFormatSubstitute(class1);
    tTable.getSd().setOutputFormat(class1.getName());
  }

  final public Properties getSchema() {
    return schema;
  }

  final public Path getPath() {
    return new Path(getTTable().getSd().getLocation());
  }

  final public String getName() {
    return getTTable().getTableName();
  }

  final public URI getDataLocation() {
    return uri;
  }

  final public Deserializer getDeserializer() {
    if (deserializer == null) {
      try {
        initSerDe();
      } catch (HiveException e) {
        LOG.error("Error in initializing serde.", e);
      }
    }
    return deserializer;
  }

  final public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  final public Class<? extends HiveOutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  final public boolean isValidSpec(Map<String, String> spec)
      throws HiveException {

    // TODO - types need to be checked.
    List<FieldSchema> partCols = getTTable().getPartitionKeys();
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
    getTTable().getParameters().put(name, value);
  }

  /**
   * getProperty
   * 
   */
  public String getProperty(String name) {
    return getTTable().getParameters().get(name);
  }

  public void setTableType(TableType tableType) {
     getTTable().setTableType(tableType.toString());
   }

  public TableType getTableType() {
     return Enum.valueOf(TableType.class, getTTable().getTableType());
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

  /**
   * @param schema
   *          the schema to set
   */
  public void setSchema(Properties schema) {
    this.schema = schema;
  }

  /**
   * @param deserializer
   *          the deserializer to set
   */
  public void setDeserializer(Deserializer deserializer) {
    this.deserializer = deserializer;
  }

  @Override
  public String toString() {
    return getTTable().getTableName();
  }

  public List<FieldSchema> getPartCols() {
    List<FieldSchema> partKeys = getTTable().getPartitionKeys();
    if (partKeys == null) {
      partKeys = new ArrayList<FieldSchema>();
      getTTable().setPartitionKeys(partKeys);
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
    List<String> bcols = getTTable().getSd().getBucketCols();
    if (bcols == null || bcols.size() == 0) {
      return null;
    }

    if (bcols.size() > 1) {
      LOG.warn(this
          + " table has more than one dimensions which aren't supported yet");
    }

    return bcols.get(0);
  }

  /**
   * @return the tTable
   */
  public org.apache.hadoop.hive.metastore.api.Table getTTable() {
    return tTable;
  }

  /**
   * @param table
   *          the tTable to set
   */
  protected void setTTable(org.apache.hadoop.hive.metastore.api.Table table) {
    tTable = table;
  }

  public void setDataLocation(URI uri2) {
    uri = uri2;
    getTTable().getSd().setLocation(uri2.toString());
  }

  public void setBucketCols(List<String> bucketCols) throws HiveException {
    if (bucketCols == null) {
      return;
    }

    for (String col : bucketCols) {
      if (!isField(col)) {
        throw new HiveException("Bucket columns " + col
            + " is not part of the table columns");
      }
    }
    getTTable().getSd().setBucketCols(bucketCols);
  }

  public void setSortCols(List<Order> sortOrder) throws HiveException {
    getTTable().getSd().setSortCols(sortOrder);
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
      return getTTable().getSd().getCols();
    } else {
      try {
        return Hive.getFieldsFromDeserializer(getName(), getDeserializer());
      } catch (HiveException e) {
        LOG
            .error("Unable to get field from serde: " + getSerializationLib(),
                e);
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
    getTTable().setPartitionKeys(partCols);
  }

  public String getDbName() {
    return getTTable().getDbName();
  }

  public int getNumBuckets() {
    return getTTable().getSd().getNumBuckets();
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
    try {
      setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>) Class
          .forName(name, true, JavaUtils.getClassLoader()));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }

  public void setOutputFormatClass(String name) throws HiveException {
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
    getTTable().getSd().setCols(fields);
  }

  public void setNumBuckets(int nb) {
    getTTable().getSd().setNumBuckets(nb);
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
    return getTTable().getSd().getSerdeInfo();
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
    return getTTable().getSd().getBucketCols();
  }

  public List<Order> getSortCols() {
    return getTTable().getSd().getSortCols();
  }

  /**
   * @return the original view text, or null if this table is not a view
   */
  public String getViewOriginalText() {
    return getTTable().getViewOriginalText();
  }

  /**
   * @param viewOriginalText
   *          the original view text to set
   */
  public void setViewOriginalText(String viewOriginalText) {
    getTTable().setViewOriginalText(viewOriginalText);
  }

  /**
   * @return the expanded view text, or null if this table is not a view
   */
  public String getViewExpandedText() {
    return getTTable().getViewExpandedText();
  }

  /**
   * @param viewExpandedText
   *          the expanded view text to set
   */
  public void setViewExpandedText(String viewExpandedText) {
    getTTable().setViewExpandedText(viewExpandedText);
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
    Table newTbl = new Table();

    newTbl.schema = schema;
    newTbl.deserializer = deserializer; // TODO: convert to SerDeInfo format

    newTbl.setTTable(getTTable().clone());
    newTbl.uri = uri;
    newTbl.inputFormatClass = inputFormatClass;
    newTbl.outputFormatClass = outputFormatClass;
    return newTbl;
  }
};
