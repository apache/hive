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
package org.apache.hadoop.hive.common;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidWriteIds {
  public static final ValidWriteIds NO_WRITE_IDS = new ValidWriteIds(-1, -1, false, null);

  public static final String MM_PREFIX = "mm";
  private static final String CURRENT_SUFFIX = ".current";

  private final static Logger LOG = LoggerFactory.getLogger(ValidWriteIds.class);

  private static final String VALID_WRITEIDS_PREFIX = "hive.valid.write.ids.";
  private final long lowWatermark, highWatermark;
  private final boolean areIdsValid;
  private final HashSet<Long> ids;
  private String source = null;

  public ValidWriteIds(
      long lowWatermark, long highWatermark, boolean areIdsValid, HashSet<Long> ids) {
    this.lowWatermark = lowWatermark;
    this.highWatermark = highWatermark;
    this.areIdsValid = areIdsValid;
    this.ids = ids;
  }

  public static ValidWriteIds createFromConf(Configuration conf, String dbName, String tblName) {
    return createFromConf(conf, dbName + "." + tblName);
  }

  public static ValidWriteIds createFromConf(Configuration conf, String fullTblName) {
    String key = createConfKey(fullTblName);
    String idStr = conf.get(key, null);
    String current = conf.get(key + CURRENT_SUFFIX, null);
    if (idStr == null || idStr.isEmpty()) return null;
    return new ValidWriteIds(idStr, current);
  }

  private static String createConfKey(String dbName, String tblName) {
    return createConfKey(dbName + "." + tblName);
  }

  private static String createConfKey(String fullName) {
    return VALID_WRITEIDS_PREFIX + fullName;
  }

  private ValidWriteIds(String src, String current) {
    // TODO: lifted from ACID config implementation... optimize if needed? e.g. ranges, base64
    String[] values = src.split(":");
    highWatermark = Long.parseLong(values[0]);
    lowWatermark = Long.parseLong(values[1]);
    if (values.length > 2) {
      areIdsValid = Long.parseLong(values[2]) > 0;
      ids = new HashSet<Long>();
      for(int i = 3; i < values.length; ++i) {
        ids.add(Long.parseLong(values[i]));
      }
      if (current != null) {
        long currentId = Long.parseLong(current);
        if (areIdsValid) {
          ids.add(currentId);
        } else {
          ids.remove(currentId);
        }
      }
    } else if (current != null) {
        long currentId = Long.parseLong(current);
        areIdsValid = true;
        ids = new HashSet<Long>();
        ids.add(currentId);
    } else {
      areIdsValid = false;
      ids = null;
    }
  }

  public static void addCurrentToConf(
      Configuration conf, String dbName, String tblName, long mmWriteId) {
    String key = createConfKey(dbName, tblName) + CURRENT_SUFFIX;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting " + key + " => " + mmWriteId);
    }
    conf.set(key, Long.toString(mmWriteId));
  }

  public void addToConf(Configuration conf, String dbName, String tblName) {
    if (source == null) {
      source = toString();
    }
    String key = createConfKey(dbName, tblName);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting " + key + " => " + source
          + " (old value was " + conf.get(key, null) + ")");
    }
    conf.set(key, source);
  }

  public static void clearConf(HiveConf conf, String dbName, String tblName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Unsetting " + createConfKey(dbName, tblName));
    }
    conf.unset(createConfKey(dbName, tblName));
  }

  public String toString() {
    // TODO: lifted from ACID config implementation... optimize if needed? e.g. ranges, base64
    StringBuilder buf = new StringBuilder();
    buf.append(highWatermark);
    buf.append(':');
    buf.append(lowWatermark);
    if (ids != null) {
      buf.append(':');
      buf.append(areIdsValid ? 1 : 0);
      for (long id : ids) {
        buf.append(':');
        buf.append(id);
      }
    }
    return buf.toString();
  }

  public boolean isValid(long writeId) {
    if (writeId < 0) throw new RuntimeException("Incorrect write ID " + writeId);
    if (writeId <= lowWatermark) return true;
    if (writeId >= highWatermark) return false;
    return ids != null && (areIdsValid == ids.contains(writeId));
  }

  public static String getMmFilePrefix(long mmWriteId) {
    return MM_PREFIX + "_" + mmWriteId;
  }


  public static class IdPathFilter implements PathFilter {
    private final String mmDirName;
    private final boolean isMatch, isIgnoreTemp;
    public IdPathFilter(long writeId, boolean isMatch) {
      this(writeId, isMatch, false);
    }
    public IdPathFilter(long writeId, boolean isMatch, boolean isIgnoreTemp) {
      this.mmDirName = ValidWriteIds.getMmFilePrefix(writeId);
      this.isMatch = isMatch;
      this.isIgnoreTemp = isIgnoreTemp;
    }

    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      if (name.equals(mmDirName)) {
        return isMatch;
      }
      if (isIgnoreTemp && name.length() > 0) {
        char c = name.charAt(0);
        if (c == '.' || c == '_') return false; // Regardless of isMatch, ignore this.
      }
      return !isMatch;
    }
  }

  public static class AnyIdDirFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      if (!name.startsWith(MM_PREFIX + "_")) return false;
      String idStr = name.substring(MM_PREFIX.length() + 1);
      try {
        Long.parseLong(idStr);
      } catch (NumberFormatException ex) {
        return false;
      }
      return true;
    }
  }
  public static Long extractWriteId(Path file) {
    String fileName = file.getName();
    String[] parts = fileName.split("_", 3);
    if (parts.length < 2 || !MM_PREFIX.equals(parts[0])) {
      LOG.info("Cannot extract write ID for a MM table: " + file
          + " (" + Arrays.toString(parts) + ")");
      return null;
    }
    long writeId = -1;
    try {
      writeId = Long.parseLong(parts[1]);
    } catch (NumberFormatException ex) {
      LOG.info("Cannot extract write ID for a MM table: " + file
          + "; parsing " + parts[1] + " got " + ex.getMessage());
      return null;
    }
    return writeId;
  }

}