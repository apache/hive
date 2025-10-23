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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;

/**
 * Utility functions for working with HivePrivilegeObject
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
@Evolving
public class HivePrivilegeObjectUtils {

  /**
   * Convert list of dbnames into list of HivePrivilegeObject
   * @param dbList
   * @return
   */
  public static List<HivePrivilegeObject> getHivePrivDbObjects(String catName, List<String> dbList) {
    List<HivePrivilegeObject> objs = new ArrayList<HivePrivilegeObject>();
    for (String dbname : dbList) {
      objs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, catName, dbname, dbname));
    }
    return objs;

  }

  /**
   * Convert list of dcnames into list of HivePrivilegeObject
   * @param dcList
   * @return
   */
  public static List<HivePrivilegeObject> getHivePrivDcObjects(List<String> dcList) {
    List<HivePrivilegeObject> objs = new ArrayList<HivePrivilegeObject>();
    for (String dcname : dcList) {
      objs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATACONNECTOR, dcname));
    }
    return objs;
  }

  /**
   * A helper enabling efficient lookup of a table in a list by catalog name, database name, table name.
   * <p>When filtering a source list of tables by checking their presence in another permission list
   * using the catalog name, database name, table name, we need to avoid performing a cartesian product.
   * This product stems from checking each table from the source (n tables) by comparing it to each
   * table in the permission list (m permissions), thus an n * m complexity.</p>
   * <p>This class reduces the complexity of the lookup in the permission by sorting them and
   * using a binary search; the sort cost is m*log(m) and each lookup is log(m), the overall
   * complexity of a check for all tables is in the order of m*log(m) + n*log(m) = (m + n)*log(n),
   * close to m * log(n).</p>
   *
   */
  public abstract static class TableLookup<T> {
    /** The container. */
    private final T[] index;

    /**
     * @param table the table
     * @return the catalog name of the table
     */
    protected abstract String getCatName(T table);

    /**
     * @param table the table
     * @return the database name of the table
     */
    protected abstract String getDbName(T table);

    /**
     * @param table the table
     * @return the table name of the table
     */
    protected abstract String getTableName(T table);

    /**
     * Compares a table to another by names.
     * @param table the table
     * @param arg the argument table
     * @return &lt; 0, 0, &gt; 0
     */
    private int compareNames(final T table, final T arg) {
      return compareNames(table, getCatName(arg), getDbName(arg), getTableName(arg));
    }

    /**
     * Compares a table to names.
     * @param table the table
     * @param catName the argument catalog name
     * @param dbName the argument database name
     * @param dbName the argument table name
     * @return &lt; 0, 0, &gt; 0
     */
    private int compareNames(final T table, final String catName, final String dbName, final String tableName) {
      int cmp = getCatName(table).compareTo(catName);
      if (cmp == 0 && dbName != null) {
        cmp = getDbName(table).compareTo(dbName);
      }
      if (cmp == 0 && tableName != null) {
        cmp = getTableName(table).compareTo(tableName);
      }
      return cmp;
    }

    /**
     * Creates a ByName
     * @param tables the tables to consider as a clique
     */
    protected TableLookup(List<T> tables) {
      if (tables != null) {
        index = tables.toArray((T[]) new Object[0]);
        // sort them by names
        Arrays.sort(index, this::compareNames);
      } else {
        index = (T[]) new Object[0];
      }
    }

    /**
     * Lookup using dichotomy using order described by catalog name, database name, table name.
     * @param catName the catalog name
     * @param dbName the database name
     * @param tableName the table name
     * @return the table if found in the index, null otherwise
     */
    public final T lookup(final String catName, final String dbName, final String tableName) {
      int low = 0;
      int high = index.length - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        T item = index[mid];
        int cmp = compareNames(item, catName, dbName, tableName);
        if (cmp < 0) {
          low = mid + 1;
        } else if (cmp > 0) {
          high = mid - 1;
        } else {
          return item; // key found
        }
      }
      return null;  // key not found.
    }

    /**
     * Checks whether a given table is present in this set.
     * @param tt the table to check
     * @return true if the set contains an item having the same database and table name
     */
    public final boolean contains(T tt) {
      return lookup(getCatName(tt), getDbName(tt), getTableName(tt)) != null;
    }
  }

  /**
   * Specialized lookup for table privilege (HivePrivilegeObject)..
   */
  public static class TablePrivilegeLookup extends TableLookup<HivePrivilegeObject> {
    public TablePrivilegeLookup(List<HivePrivilegeObject> tables) {
      super(tables);
    }

    @Override protected String getCatName(HivePrivilegeObject o) {
      return o.getCatName();
    }

    @Override protected String getDbName(HivePrivilegeObject o) {
      return o.getDbname();
    }

    @Override protected String getTableName(HivePrivilegeObject o) {
      return o.getObjectName();
    }
  }
}
