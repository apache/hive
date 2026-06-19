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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.util.List;
import java.util.Map;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * A description of the table to create.
 */
@XmlRootElement
public class TableDesc extends GroupPermissionsDesc {
  public boolean external = false;
  public boolean ifNotExists = false;
  public String table;
  public String comment;
  public List<ColumnDesc> columns;
  public List<ColumnDesc> partitionedBy;
  public ClusteredByDesc clusteredBy;
  public StorageFormatDesc format;
  public String location;
  public Map<String, String> tableProperties;

  /**
   * Create a new TableDesc
   */
  public TableDesc() {
  }

  public String toString() {
    return String.format("TableDesc(table=%s, columns=%s)", table, columns);
  }

  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (!(o instanceof TableDesc))
      return false;
    TableDesc that = (TableDesc) o;
    return xequals(this.external, that.external)
      && xequals(this.ifNotExists, that.ifNotExists)
      && xequals(this.table, that.table)
      && xequals(this.comment, that.comment)
      && xequals(this.columns, that.columns)
      && xequals(this.partitionedBy, that.partitionedBy)
      && xequals(this.clusteredBy, that.clusteredBy)
      && xequals(this.format, that.format)
      && xequals(this.location, that.location)
      && xequals(this.tableProperties, that.tableProperties)
      && super.equals(that)
      ;
  }

  /**
   * How to cluster the table.
   */
  @XmlRootElement
  public static class ClusteredByDesc {
    public List<String> columnNames;
    public List<ClusterSortOrderDesc> sortedBy;
    public int numberOfBuckets;

    public ClusteredByDesc() {
    }

    public String toString() {
      String fmt
        = "ClusteredByDesc(columnNames=%s, sortedBy=%s, numberOfBuckets=%s)";
      return String.format(fmt, columnNames, sortedBy, numberOfBuckets);
    }

    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof ClusteredByDesc))
        return false;
      ClusteredByDesc that = (ClusteredByDesc) o;
      return xequals(this.columnNames, that.columnNames)
        && xequals(this.sortedBy, that.sortedBy)
        && xequals(this.numberOfBuckets, that.numberOfBuckets)
        ;
    }
  }

  /**
   * The clustered sort order.
   */
  @XmlRootElement
  public static class ClusterSortOrderDesc {
    public String columnName;
    public SortDirectionDesc order;

    public ClusterSortOrderDesc() {
    }

    public ClusterSortOrderDesc(String columnName, SortDirectionDesc order) {
      this.columnName = columnName;
      this.order = order;
    }

    public String toString() {
      return String
        .format("ClusterSortOrderDesc(columnName=%s, order=%s)",
          columnName, order);
    }

    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof ClusterSortOrderDesc))
        return false;
      ClusterSortOrderDesc that = (ClusterSortOrderDesc) o;
      return xequals(this.columnName, that.columnName)
        && xequals(this.order, that.order)
        ;
    }
  }

  /**
   * Ther ASC or DESC sort order.
   */
  @XmlRootElement
  public static enum SortDirectionDesc {
    ASC, DESC
  }

  /**
   * The storage format.
   */
  @XmlRootElement
  public static class StorageFormatDesc {
    public RowFormatDesc rowFormat;
    public String storedAs;
    public StoredByDesc storedBy;

    public StorageFormatDesc() {
    }

    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof StorageFormatDesc))
        return false;
      StorageFormatDesc that = (StorageFormatDesc) o;
      return xequals(this.rowFormat, that.rowFormat)
        && xequals(this.storedAs, that.storedAs)
        && xequals(this.storedBy, that.storedBy)
        ;
    }
  }

  /**
   * The Row Format.
   */
  @XmlRootElement
  public static class RowFormatDesc {
    public String fieldsTerminatedBy;
    public String collectionItemsTerminatedBy;
    public String mapKeysTerminatedBy;
    public String linesTerminatedBy;
    public SerdeDesc serde;

    public RowFormatDesc() {
    }

    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof RowFormatDesc))
        return false;
      RowFormatDesc that = (RowFormatDesc) o;
      return xequals(this.fieldsTerminatedBy, that.fieldsTerminatedBy)
        && xequals(this.collectionItemsTerminatedBy,
          that.collectionItemsTerminatedBy)
        && xequals(this.mapKeysTerminatedBy, that.mapKeysTerminatedBy)
        && xequals(this.linesTerminatedBy, that.linesTerminatedBy)
        && xequals(this.serde, that.serde)
        ;
    }
  }

  /**
   * The SERDE Row Format.
   */
  @XmlRootElement
  public static class SerdeDesc {
    public String name;
    public Map<String, String> properties;

    public SerdeDesc() {
    }

    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof SerdeDesc))
        return false;
      SerdeDesc that = (SerdeDesc) o;
      return xequals(this.name, that.name)
        && xequals(this.properties, that.properties)
        ;
    }
  }

  /**
   * How to store the table.
   */
  @XmlRootElement
  public static class StoredByDesc {
    public String className;
    public Map<String, String> properties;

    public StoredByDesc() {
    }

    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof StoredByDesc))
        return false;
      StoredByDesc that = (StoredByDesc) o;
      return xequals(this.className, that.className)
        && xequals(this.properties, that.properties)
        ;
    }
  }

}
