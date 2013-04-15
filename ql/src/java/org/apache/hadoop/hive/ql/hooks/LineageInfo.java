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

package org.apache.hadoop.hive.ql.hooks;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * This class contains the lineage information that is passed
 * to the PreExecution hook.
 */
public class LineageInfo implements Serializable {

  /**
   * Serial version id.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Enum to track dependency. This enum has the following values:
   * 1. SIMPLE - Indicates that the column is derived from another table column
   *             with no transformations e.g. T2.c1 = T1.c1.
   * 2. EXPRESSION - Indicates that the column is derived from a UDF, UDAF, UDTF or
   *                 set operations like union on columns on other tables
   *                 e.g. T2.c1 = T1.c1 + T3.c1.
   * 4. SCRIPT - Indicates that the column is derived from the output
   *             of a user script through a TRANSFORM, MAP or REDUCE syntax
   *             or from the output of a PTF chain execution.
   */
  public static enum DependencyType {
    SIMPLE, EXPRESSION, SCRIPT
  }

  /**
   * Table or Partition data container. We need this class because the output
   * of the query can either go to a table or a partition within a table. The
   * data container class subsumes both of these.
   */
  public static class DataContainer implements Serializable {

    /**
     * Serial version id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The table in case this container is a table.
     */
    private final Table tab;

    /**
     * The partition in case this container is a partition.
     */
    private final Partition part;

    /**
     * Constructor for non partitioned tables.
     *
     * @param tab The associated table.
     */
    public DataContainer(Table tab) {
      this.tab = tab;
      this.part = null;
    }

    /**
     * Constructor for a partitioned tables partition.
     *
     * @param part The associated partition.
     */
    public DataContainer(Table tab, Partition part) {
      this.tab = tab;
      this.part = part;
    }

    /**
     * Returns true in case this data container is a partition.
     *
     * @return boolean TRUE if the container is a table partition.
     */
    public boolean isPartition() {
      return (part != null);
    }

    public Table getTable() {
      return this.tab;
    }

    public Partition getPartition() {
      return this.part;
    }
  }

  /**
   * Class that captures the lookup key for the dependency. The dependency
   * is from (DataContainer, FieldSchema) to a Dependency structure. This
   * class captures the (DataContainer, FieldSchema) tuple.
   */
  public static class DependencyKey implements Serializable {

    /**
     * Serial version id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The data container for this key.
     */
    private final DataContainer dc;

    /**
     * The field schema for this key.
     */
    private final FieldSchema fld;

    /**
     * Constructor.
     *
     * @param dc The associated data container.
     * @param fld The associated field schema.
     */
    public DependencyKey(DataContainer dc, FieldSchema fld) {
      this.dc = dc;
      this.fld = fld;
    }

    public DataContainer getDataContainer() {
      return this.dc;
    }

    public FieldSchema getFieldSchema() {
      return this.fld;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((dc == null) ? 0 : dc.hashCode());
      result = prime * result + ((fld == null) ? 0 : fld.hashCode());
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
      DependencyKey other = (DependencyKey) obj;
      if (dc != other.dc) {
        return false;
      }
      if (fld != other.fld) {
        return false;
      }
      return true;
    }
  }

  /**
   * Base Column information.
   */
  public static class BaseColumnInfo implements Serializable {

    /**
     * Serial version id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The table and alias info encapsulated in a different class.
     */
    private TableAliasInfo tabAlias;

    /**
     * The metastore column information. The column can be null
     * and that denotes that the expression is dependent on the row
     * of the table and not particular column. This can happen in case
     * of count(1).
     */
    private FieldSchema column;

    /**
     * @return the tabAlias
     */
    public TableAliasInfo getTabAlias() {
      return tabAlias;
    }

    /**
     * @param tabAlias the tabAlias to set
     */
    public void setTabAlias(TableAliasInfo tabAlias) {
      this.tabAlias = tabAlias;
    }

    /**
     * @return the column
     */
    public FieldSchema getColumn() {
      return column;
    }

    /**
     * @param column the column to set
     */
    public void setColumn(FieldSchema column) {
      this.column = column;
    }
  }

  public static class TableAliasInfo implements Serializable {

    /**
     * Serail version id.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The alias for the table.
     */
    private String alias;

    /**
     * The metastore table information.
     */
    private Table table;

    /**
     * @return the alias
     */
    public String getAlias() {
      return alias;
    }

    /**
     * @param alias the alias to set
     */
    public void setAlias(String alias) {
      this.alias = alias;
    }

    /**
     * @return the table
     */
    public Table getTable() {
      return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(Table table) {
      this.table = table;
    }
  }

  /**
   * This class tracks the dependency information for the base column.
   */
  public static class Dependency implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * The type of dependency.
     */
    private DependencyType type;

    /**
     * Expression string for the dependency.
     */
    private String expr;

    /**
     * The list of base columns that the particular column depends on.
     */
    private List<BaseColumnInfo> baseCols;

    /**
     * @return the type
     */
    public DependencyType getType() {
      return type;
    }

    /**
     * @param type the type to set
     */
    public void setType(DependencyType type) {
      this.type = type;
    }

    /**
     * @return the expr
     */
    public String getExpr() {
      return expr;
    }

    /**
     * @param expr the expr to set
     */
    public void setExpr(String expr) {
      this.expr = expr;
    }

    /**
     * @return the baseCols
     */
    public List<BaseColumnInfo> getBaseCols() {
      return baseCols;
    }

    /**
     * @param baseCols the baseCols to set
     */
    public void setBaseCols(List<BaseColumnInfo> baseCols) {
      this.baseCols = baseCols;
    }
  }

  /**
   * The map contains an index from the (datacontainer, columnname) to the
   * dependency vector for that tuple. This is used to generate the
   * dependency vectors during the walk of the operator tree.
   */
  protected Map<DependencyKey, Dependency> index;

  /**
   * Constructor.
   */
  public LineageInfo() {
    index = new LinkedHashMap<DependencyKey, Dependency>();
  }

  /**
   * Gets the dependency for a table, column tuple.
   * @param dc The data container of the column whose dependency is being inspected.
   * @param col The column whose dependency is being inspected.
   * @return Dependency for that particular table, column tuple.
   *         null if no dependency is found.
   */
  public Dependency getDependency(DataContainer dc, FieldSchema col) {
    return index.get(new DependencyKey(dc, col));
  }

  /**
   * Puts the dependency for a table, column tuple.
   * @param dc The datacontainer whose dependency is being inserted.
   * @param col The column whose dependency is being inserted.
   * @param dep The dependency.
   */
  public void putDependency(DataContainer dc, FieldSchema col, Dependency dep) {
    index.put(new DependencyKey(dc, col), dep);
  }

  /**
   * Gets the entry set on this structure.
   *
   * @return LineageInfo entry set
   */
  public Set<Map.Entry<DependencyKey, Dependency>> entrySet() {
    return index.entrySet();
  }
}
