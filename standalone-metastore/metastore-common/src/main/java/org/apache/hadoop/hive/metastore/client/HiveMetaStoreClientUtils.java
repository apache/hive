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

package org.apache.hadoop.hive.metastore.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HiveMetaStoreClientUtils {
  public static Partition deepCopy(Partition partition) {
    Partition copy = null;
    if (partition != null) {
      copy = new Partition(partition);
    }
    return copy;
  }

  public static Database deepCopy(Database database) {
    Database copy = null;
    if (database != null) {
      copy = new Database(database);
    }
    return copy;
  }

  public static Table deepCopy(Table table) {
    Table copy = null;
    if (table != null) {
      copy = new Table(table);
    }
    return copy;
  }

  public static Type deepCopy(Type type) {
    Type copy = null;
    if (type != null) {
      copy = new Type(type);
    }
    return copy;
  }

  public static FieldSchema deepCopy(FieldSchema schema) {
    FieldSchema copy = null;
    if (schema != null) {
      copy = new FieldSchema(schema);
    }
    return copy;
  }

  public static Function deepCopy(Function func) {
    Function copy = null;
    if (func != null) {
      copy = new Function(func);
    }
    return copy;
  }

  public static PrincipalPrivilegeSet deepCopy(PrincipalPrivilegeSet pps) {
    PrincipalPrivilegeSet copy = null;
    if (pps != null) {
      copy = new PrincipalPrivilegeSet(pps);
    }
    return copy;
  }

  public static List<Partition> deepCopyPartitions(List<Partition> partitions) {
    return deepCopyPartitions(partitions, null);
  }

  public static List<Partition> deepCopyPartitions(Collection<Partition> src, List<Partition> dest) {
    if (src == null) {
      return dest;
    }
    if (dest == null) {
      dest = new ArrayList<Partition>(src.size());
    }
    for (Partition part : src) {
      dest.add(deepCopy(part));
    }
    return dest;
  }

  public static List<Table> deepCopyTables(List<Table> tables) {
    List<Table> copy = null;
    if (tables != null) {
      copy = new ArrayList<Table>();
      for (Table tab : tables) {
        copy.add(deepCopy(tab));
      }
    }
    return copy;
  }

  public static List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {
    List<FieldSchema> copy = null;
    if (schemas != null) {
      copy = new ArrayList<FieldSchema>();
      for (FieldSchema schema : schemas) {
        copy.add(deepCopy(schema));
      }
    }
    return copy;
  }

  /**
   * This method is called to get the ValidWriteIdList in order to send the same in HMS get_* APIs,
   * if the validWriteIdList is not explicitly passed (as a method argument) to the HMS APIs.
   * This method returns the ValidWriteIdList based on the VALID_TABLES_WRITEIDS_KEY key.
   * Since, VALID_TABLES_WRITEIDS_KEY is set during the lock acquisition phase after query compilation
   * ( DriverTxnHandler.acquireLocks -&gt; recordValidWriteIds -&gt; setValidWriteIds ),
   * this only covers a subset of cases, where we invoke get_* APIs after query compilation,
   * if the validWriteIdList is not explicitly passed (as a method argument) to the HMS APIs.
   */
  public static String getValidWriteIdList(String dbName, String tblName, Configuration conf) {
    String validTableWriteIdsKey = conf.get(ValidTxnWriteIdList.VALID_TABLES_WRITEIDS_KEY);
    if (validTableWriteIdsKey == null) {
      return null;
    }

    ValidTxnWriteIdList validTxnWriteIdList = ValidTxnWriteIdList.fromValue(validTableWriteIdsKey);
    ValidWriteIdList writeIdList =
        validTxnWriteIdList.getTableValidWriteIdList(TableName.getDbTable(dbName, tblName));
    return writeIdList != null ? writeIdList.toString() : null;
  }

  public static short shrinkMaxtoShort(int max) {
    if (max < 0) {
      return -1;
    } else if (max <= Short.MAX_VALUE) {
      return (short)max;
    } else {
      return Short.MAX_VALUE;
    }
  }
}

