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
import java.util.Collection;
import java.util.Iterator;
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
  public static List<HivePrivilegeObject> getHivePrivDbObjects(List<String> dbList) {
    List<HivePrivilegeObject> objs = new ArrayList<HivePrivilegeObject>();
    for (String dbname : dbList) {
      objs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATABASE, dbname, dbname));
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
      objs.add(new HivePrivilegeObject(HivePrivilegeObjectType.DATACONNECTOR, null, dcname));
    }
    return objs;
  }


  /**
   * An index on a table list by dbName, tableName.
   */
  public abstract static class TableIndex<T> {
    private final T[] index;
    protected abstract String getDbName(T item);
    protected abstract String getTableName(T item);
    protected TableIndex(List<T> tables) {
      if (tables != null) {
        index = tables.toArray((T[]) new Object[0]);
        // sort them by dbname, tblname
        Arrays.sort(index, (left, right)->{
          int cmp = getDbName(left).compareTo(getDbName(right));
          return cmp != 0
              ? cmp
              : getTableName(left).compareTo(getTableName(right));
        });
      } else {
        index = (T[]) new Object[0];
      }
    }

    /**
     * Lookup using dichotomy using order described by Name, tblName
     * @param dbName the database name
     * @param tableName the table name
     * @return the table if found in the index, null otherwise
     */
    public T lookup(String dbName, String tableName) {
      int low = 0;
      int high = index.length - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        T item = index[mid];
        int cmp = getDbName(item).compareTo(dbName);
        if (cmp == 0) {
          cmp = getTableName(item).compareTo(tableName);
        }
        if (cmp < 0)
          low = mid + 1;
        else if (cmp > 0)
          high = mid - 1;
        else
          return item; // key found
      }
      return null;  // key not found.
    }
  }

  /**
   * Specialized index for HivePrivilegeObject.
   */
  public static class PrivilegeIndex extends TableIndex<HivePrivilegeObject> {
    public PrivilegeIndex(List<HivePrivilegeObject> tables) {
      super(tables);
    }

    @Override
    protected String getDbName(HivePrivilegeObject item) {
      return item.getDbname();
    }

    @Override
    protected String getTableName(HivePrivilegeObject item) {
      return item.getObjectName();
    }
  }
}
