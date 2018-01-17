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
package org.apache.hadoop.hive.metastore.cache;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.cache.CachedStore.PartitionWrapper;
import org.apache.hadoop.hive.metastore.cache.CachedStore.TableWrapper;
import org.apache.hadoop.hive.metastore.utils.StringUtils;

public class CacheUtils {
  private static final String delimit = "\u0001";

  public static String buildKey(String dbName) {
    return dbName;
  }

  public static String buildKeyWithDelimit(String dbName) {
    return buildKey(dbName) + delimit;
  }

  public static String buildKey(String dbName, String tableName) {
    return dbName + delimit + tableName;
  }

  public static String buildKeyWithDelimit(String dbName, String tableName) {
    return buildKey(dbName, tableName) + delimit;
  }

  public static String buildKey(String dbName, String tableName, List<String> partVals) {
    String key = buildKey(dbName, tableName);
    if (CollectionUtils.isNotEmpty(partVals)) {
      key += delimit;
      key += String.join(delimit, partVals);
    }
    return key;
  }

  public static String buildKeyWithDelimit(String dbName, String tableName, List<String> partVals) {
    return buildKey(dbName, tableName, partVals) + delimit;
  }

  public static String buildKey(String dbName, String tableName, List<String> partVals, String colName) {
    String key = buildKey(dbName, tableName, partVals);
    return key + delimit + colName;
  }

  public static String buildKey(String dbName, String tableName, String colName) {
    String key = buildKey(dbName, tableName);
    return key + delimit + colName;
  }

  public static String[] splitTableColStats(String key) {
    return key.split(delimit);
  }

  public static Object[] splitPartitionColStats(String key) {
    Object[] result = new Object[4];
    String[] comps = key.split(delimit);
    result[0] = comps[0];
    result[1] = comps[1];
    result[2] = Arrays.asList((Arrays.copyOfRange(comps, 2, comps.length - 1)));
    result[3] = comps[comps.length-1];
    return result;
  }

  public static Object[] splitAggrColStats(String key) {
    return key.split(delimit);
  }

  static Table assemble(TableWrapper wrapper, SharedCache sharedCache) {
    Table t = wrapper.getTable().deepCopy();
    if (wrapper.getSdHash() != null) {
      StorageDescriptor sdCopy = sharedCache.getSdFromCache(wrapper.getSdHash()).deepCopy();
      if (sdCopy.getBucketCols() == null) {
        sdCopy.setBucketCols(Collections.emptyList());
      }
      if (sdCopy.getSortCols() == null) {
        sdCopy.setSortCols(Collections.emptyList());
      }
      if (sdCopy.getSkewedInfo() == null) {
        sdCopy.setSkewedInfo(new SkewedInfo(Collections.emptyList(),
          Collections.emptyList(), Collections.emptyMap()));
      }
      sdCopy.setLocation(wrapper.getLocation());
      sdCopy.setParameters(wrapper.getParameters());
      t.setSd(sdCopy);
    }
    return t;
  }

  static Partition assemble(PartitionWrapper wrapper, SharedCache sharedCache) {
    Partition p = wrapper.getPartition().deepCopy();
    if (wrapper.getSdHash() != null) {
      StorageDescriptor sdCopy = sharedCache.getSdFromCache(wrapper.getSdHash()).deepCopy();
      if (sdCopy.getBucketCols() == null) {
        sdCopy.setBucketCols(Collections.emptyList());
      }
      if (sdCopy.getSortCols() == null) {
        sdCopy.setSortCols(Collections.emptyList());
      }
      if (sdCopy.getSkewedInfo() == null) {
        sdCopy.setSkewedInfo(new SkewedInfo(Collections.emptyList(),
          Collections.emptyList(), Collections.emptyMap()));
      }
      sdCopy.setLocation(wrapper.getLocation());
      sdCopy.setParameters(wrapper.getParameters());
      p.setSd(sdCopy);
    }
    return p;
  }

  public static boolean matches(String name, String pattern) {
    String[] subpatterns = pattern.trim().split("\\|");
    for (String subpattern : subpatterns) {
      subpattern = "(?i)" + subpattern.replaceAll("\\?", ".{1}").replaceAll("\\*", ".*")
          .replaceAll("\\^", "\\\\^").replaceAll("\\$", "\\\\$");
      if (Pattern.matches(subpattern, StringUtils.normalizeIdentifier(name))) {
        return true;
      }
    }
    return false;
  }
}
