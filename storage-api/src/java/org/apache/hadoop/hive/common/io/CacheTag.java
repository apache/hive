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
package org.apache.hadoop.hive.common.io;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * Used for identifying the related object of the buffer stored in cache.
 * Comes in 3 flavours to optimize for minimal memory overhead:
 * - TableCacheTag for tables without partitions: DB/table level
 * - SinglePartitionCacheTag for tables with 1 partition level: DB/table/1st_partition
 * - MultiPartitionCacheTag for tables with &gt; 1 partition levels:
 *     DB/table/1st_partition/.../nth_partition .
 */
public abstract class CacheTag implements Comparable<CacheTag> {

  private static final String ENCODING = "UTF-8";

  /**
   * Prepended by DB name and '.' .
   */
  protected final String tableName;

  private CacheTag(String tableName) {
    this.tableName = tableName.intern();
  }

  public String getTableName() {
    return tableName;
  }

  @Override
  public int compareTo(CacheTag o) {
    if (o == null) {
      return 1;
    }
    return tableName.compareTo(o.tableName);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof CacheTag)) {
      return false;
    } else {
      return this.compareTo((CacheTag) obj) == 0;
    }
  }

  @Override
  public int hashCode() {
    int res = tableName.hashCode();
    return res;
  }

  public static final CacheTag build(String tableName) {
    if (StringUtils.isEmpty(tableName)) {
      throw new IllegalArgumentException();
    }
    return new TableCacheTag(tableName);
  }

  public static final CacheTag build(String tableName, LinkedHashMap<String, String> partDescMap) {
    if (StringUtils.isEmpty(tableName) || partDescMap == null || partDescMap.isEmpty()) {
      throw new IllegalArgumentException();
    }

    String[] partDescs = new String[partDescMap.size()];
    int i = 0;

    for (Map.Entry<String, String> entry : partDescMap.entrySet()) {
      partDescs[i++] = encodePartDesc(entry.getKey(), entry.getValue());
    }

    if (partDescs.length == 1) {
      return new SinglePartitionCacheTag(tableName, partDescs[0]);
    } else {
      // In this case it must be >1
      return new MultiPartitionCacheTag(tableName, partDescs);
    }
  }

  public static final CacheTag build(String tableName, List<String> partDescs) {
    if (StringUtils.isEmpty(tableName) || partDescs == null || partDescs.isEmpty()) {
      throw new IllegalArgumentException();
    }

    if (partDescs.size() == 1) {
      return new SinglePartitionCacheTag(tableName, partDescs.get(0));
    } else {
      // In this case it must be >1
      return new MultiPartitionCacheTag(tableName, partDescs.toArray(new String[0]));
    }
  }

  /**
   * Constructs a (fake) parent CacheTag instance by walking back in the hierarchy i.e. stepping
   * from inner to outer partition levels, then producing a CacheTag for the table and finally
   * the DB.
   */
  public static final CacheTag createParentCacheTag(CacheTag tag) {
    if (tag == null) {
      throw new IllegalArgumentException();
    }

    if (tag instanceof MultiPartitionCacheTag) {
      MultiPartitionCacheTag multiPartitionCacheTag = (MultiPartitionCacheTag) tag;
      if (multiPartitionCacheTag.partitionDesc.length > 2) {
        String[] subList = new String[multiPartitionCacheTag.partitionDesc.length - 1];
        for (int i = 0; i < subList.length; ++i) {
          subList[i] = multiPartitionCacheTag.partitionDesc[i];
        }
        return new MultiPartitionCacheTag(multiPartitionCacheTag.tableName, subList);
      } else {
        return new SinglePartitionCacheTag(multiPartitionCacheTag.tableName,
            multiPartitionCacheTag.partitionDesc[0]);
      }
    }

    if (tag instanceof SinglePartitionCacheTag) {
      return new TableCacheTag(tag.tableName);
    } else {
      // DB level
      int ix = tag.tableName.indexOf(".");
      if (ix <= 0) {
        return null;
      }
      return new TableCacheTag(tag.tableName.substring(0, ix));
    }

  }

  /**
   * CacheTag for tables without partitions.
   */
  public static final class TableCacheTag extends CacheTag {

    private TableCacheTag(String tableName) {
      super(tableName);
    }

    @Override
    public int compareTo(CacheTag o) {
      if (o == null) {
        return 1;
      }
      if (o instanceof SinglePartitionCacheTag || o instanceof MultiPartitionCacheTag) {
        return -1;
      } else {
        return super.compareTo(o);
      }
    }

  }

  /**
   * CacheTag for tables with partitions.
   */
  public abstract static class PartitionCacheTag extends CacheTag {

    private PartitionCacheTag(String tableName) {
      super(tableName);
    }

    /**
     * Returns a pretty printed String version of the partitionDesc in the format of p1=v1/p2=v2...
     * @return the pretty printed String
     */
    public abstract String partitionDescToString();

    /**
     * Returns a map of partition keys and values built from the information of this CacheTag.
     * @return the map
     */
    public abstract LinkedHashMap<String, String> getPartitionDescMap();

    /**
     * Returns the encoded partition desc. Mainly for serialization, to short circuit the encoding/decoding process.
     */
    public abstract String[] getEncodedPartitionDesc();

  }

  /**
   * CacheTag for tables with exactly one partition level.
   */
  public static final class SinglePartitionCacheTag extends PartitionCacheTag {

    private final String partitionDesc;

    private SinglePartitionCacheTag(String tableName, String partitionDesc) {
      super(tableName);
      if (StringUtils.isEmpty(partitionDesc)) {
        throw new IllegalArgumentException();
      }
      this.partitionDesc = partitionDesc.intern();
    }

    @Override
    public String partitionDescToString() {
      return String.join("=", CacheTag.decodePartDesc(partitionDesc));
    }

    @Override
    public LinkedHashMap<String, String> getPartitionDescMap() {
      LinkedHashMap<String, String> result = new LinkedHashMap<>();
      String[] partition = CacheTag.decodePartDesc(partitionDesc);
      result.put(partition[0], partition[1]);
      return result;
    }

    @Override public String[] getEncodedPartitionDesc() {
      return new String[] { partitionDesc };
    }

    @Override
    public int compareTo(CacheTag o) {
      if (o == null) {
        return 1;
      }
      if (o instanceof TableCacheTag) {
        return 1;
      } else if (o instanceof MultiPartitionCacheTag) {
        return -1;
      }
      SinglePartitionCacheTag other = (SinglePartitionCacheTag) o;
      int tableNameDiff = super.compareTo(other);
      if (tableNameDiff != 0) {
        return tableNameDiff;
      } else {
        return partitionDesc.compareTo(other.partitionDesc);
      }
    }

    @Override
    public int hashCode() {
      return super.hashCode() + partitionDesc.hashCode();
    }
  }

  /**
   * CacheTag for tables with more than one partition level.
   */
  public static final class MultiPartitionCacheTag extends PartitionCacheTag {

    private final String[] partitionDesc;

    private MultiPartitionCacheTag(String tableName, String[] partitionDesc) {
      super(tableName);
      if (partitionDesc != null && partitionDesc.length > 1) {
        for (int i = 0; i < partitionDesc.length; ++i) {
          partitionDesc[i] = partitionDesc[i].intern();
        }
        this.partitionDesc = partitionDesc;
      } else {
        throw new IllegalArgumentException();
      }
    }

    @Override
    public int compareTo(CacheTag o) {
      if (o == null) {
        return 1;
      }
      if (o instanceof TableCacheTag || o instanceof SinglePartitionCacheTag) {
        return 1;
      }
      MultiPartitionCacheTag other = (MultiPartitionCacheTag) o;
      int tableNameDiff = super.compareTo(other);
      if (tableNameDiff != 0) {
        return tableNameDiff;
      } else {
        int sizeDiff = partitionDesc.length - other.partitionDesc.length;
        if (sizeDiff != 0) {
          return sizeDiff;
        } else {
          for (int i = 0; i < partitionDesc.length; ++i) {
            int partDiff = partitionDesc[i].compareTo(other.partitionDesc[i]);
            if (partDiff != 0) {
              return partDiff;
            }
          }
          return 0;
        }
      }
    }

    @Override
    public int hashCode() {
      int res = super.hashCode();
      for (String p : partitionDesc) {
        res += p.hashCode();
      }
      return res;
    }

    @Override
    public String partitionDescToString() {
      StringBuilder sb = new StringBuilder();
      for (String partDesc : partitionDesc) {
        String[] partition = CacheTag.decodePartDesc(partDesc);
        sb.append(partition[0]).append('=').append(partition[1]);
        sb.append('/');
      }
      sb.deleteCharAt(sb.length() - 1);
      return sb.toString();
    }

    @Override
    public LinkedHashMap<String, String> getPartitionDescMap() {
      LinkedHashMap<String, String> result = new LinkedHashMap<>();
      for (String partDesc : partitionDesc) {
        String[] partition = CacheTag.decodePartDesc(partDesc);
        result.put(partition[0], partition[1]);
      }
      return result;
    }

    @Override
    public String[] getEncodedPartitionDesc() {
      return Arrays.copyOf(partitionDesc, partitionDesc.length);
    }
  }

  /**
   * Combines partition key and value Strings into one by encoding each and concating with '=' .
   * @param partKey
   * @param partVal
   * @return
   */
  private static String encodePartDesc(String partKey, String partVal) {
    try {
      StringBuilder sb = new StringBuilder();
      sb.append(
          URLEncoder.encode(partKey, ENCODING))
          .append('=')
          .append(URLEncoder.encode(partVal, ENCODING));
      return sb.toString();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Splits and decodes an a partition desc String encoded by encodePartDesc().
   * @param partDesc
   * @return
   */
  private static String[] decodePartDesc(String partDesc) {
    try {
      String[] encodedPartDesc = partDesc.split("=");
      assert encodedPartDesc.length == 2;
      return new String[] {
          URLDecoder.decode(encodedPartDesc[0], ENCODING),
          URLDecoder.decode(encodedPartDesc[1], ENCODING)};
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}

