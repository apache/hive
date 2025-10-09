/*
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

package org.apache.hadoop.hive.serde2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This abstract class is the superclass of all classes that can serialize and
 * de-serialize Hadoop {@link Writable} objects.
 */
public abstract class AbstractSerDe implements Deserializer, Serializer {

  protected static final Logger log = LoggerFactory.getLogger(AbstractSerDe.class);

  protected Optional<Configuration> configuration;
  protected Properties properties;
  protected Properties tableProperties;
  protected Optional<Properties> partitionProperties;

  private List<String> columnNames;
  private List<TypeInfo> columnTypes;
  private List<String> columnComments;

  private List<String> partitionColumnNames;
  private List<TypeInfo> partitionColumnTypes;
  private List<String> partitionColumnComments;

  /**
   * Initialize the SerDe. By default, this will use one set of properties,
   * either the table properties or the partition properties. If a SerDe needs
   * access to both sets, it should override this method.
   *
   * Eventually, once all SerDes have implemented this method, we should convert
   * it to an abstract method.
   *
   * @param configuration Hadoop configuration
   * @param tableProperties Table properties
   * @param partitionProperties Partition properties (may be {@code null} if
   *          table has no partitions)
   * @throws NullPointerException if tableProperties is {@code null}
   * @throws SerDeException if SerDe fails to initialize
   */
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    this.configuration = Optional.ofNullable(configuration);
    this.tableProperties = Objects.requireNonNull(tableProperties);
    this.partitionProperties = Optional.ofNullable(partitionProperties);
    this.properties = SerDeUtils.createOverlayedProperties(tableProperties, partitionProperties);
    this.columnNames = parseColumnNames();
    this.columnTypes = parseColumnTypes();
    this.columnComments = parseColumnComments(serdeConstants.LIST_COLUMN_COMMENTS);
    this.partitionColumnNames = parseColumnNames(serdeConstants.LIST_PARTITION_COLUMNS);
    this.partitionColumnTypes = parseColumnTypes(serdeConstants.LIST_PARTITION_COLUMN_TYPES);
    this.partitionColumnComments = parseColumnComments(serdeConstants.LIST_PARTITION_COLUMN_COMMENTS);

    Preconditions.checkArgument(this.columnNames.size() == this.columnTypes.size(),
        "Column names must match count of column types");

    log.debug("{} initialized: [{}][{}]", getClass().getName(), this.configuration, this.properties);
  }

  protected List<String> parseColumnNames() {
    return parseColumnNames(serdeConstants.LIST_COLUMNS);
  }

  protected List<String> parseColumnNames(String key) {
    final String columnNameProperty = this.properties.getProperty(key, "");
    final String columnNameDelimiter =
        this.properties.getProperty(serdeConstants.COLUMN_NAME_DELIMITER, String.valueOf(SerDeUtils.COMMA));

    return columnNameProperty.isEmpty() ? Collections.emptyList()
        : Collections.unmodifiableList(Arrays.asList(columnNameProperty.split(columnNameDelimiter)));
  }

  protected List<TypeInfo> parseColumnTypes() {
    return parseColumnTypes(serdeConstants.LIST_COLUMN_TYPES);
  }

  protected List<TypeInfo> parseColumnTypes(String key) {
    final String columnTypeProperty = this.properties.getProperty(key, "");

    return columnTypeProperty.isEmpty() ? Collections.emptyList()
        : Collections.unmodifiableList(TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty));
  }

  protected List<String> parseColumnComments(String key) {
    final String columnCommentProperty = this.properties.getProperty(key, "");

    return columnCommentProperty.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(
        Arrays.asList(columnCommentProperty.split(Character.toString(ColumnType.COLUMN_COMMENTS_DELIMITER))));
  }

  /**
   * Returns the Writable class that would be returned by the serialize method.
   * This is used to initialize SequenceFile header.
   */
  public abstract Class<? extends Writable> getSerializedClass();

  /**
   * Returns statistics collected when serializing.
   *
   * @return A SerDeStats object or {@code null} if stats are not supported by
   *         this SerDe. Returns statistics collected when serializing
   */
  public SerDeStats getSerDeStats() {
    return null;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new SerDeException("Serialize is not implemented");
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    throw new SerDeException("Deserialize is not implemented");
  }

  /**
   * @return Whether the SerDe that can store schema both inside and outside of
   *         metastore does, in fact, store it inside metastore, based on table
   *         parameters.
   */
  public boolean shouldStoreFieldsInMetastore(Map<String, String> tableParams) {
    return false;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  public List<String> getColumnComments() {
    return columnComments;
  }

  public List<String> getPartitionColumnNames() {
    return partitionColumnNames;
  }

  public List<TypeInfo> getPartitionColumnTypes() {
    return partitionColumnTypes;
  }

  public List<String> getPartitionColumnComments() {
    return partitionColumnComments;
  }

  public Optional<Configuration> getConfiguration() {
    return configuration;
  }

  @Override
  public String toString() {
    return "AbstractSerDe [log=" + log + ", configuration=" + configuration + ", properties=" + properties
        + ", tableProperties=" + tableProperties + ", partitionProperties=" + partitionProperties + ", columnNames="
        + columnNames + ", columnTypes=" + columnTypes + ", getClass()=" + getClass() + "]";
  }

}
