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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValidWriteIds {
  public static final ValidWriteIds NO_WRITE_IDS = new ValidWriteIds(-1, -1, false, null);

  private static final String MM_PREFIX = "mm";

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
    String idStr = conf.get(createConfKey(fullTblName), null);
    if (idStr == null || idStr.isEmpty()) return null;
    return new ValidWriteIds(idStr);
  }

  private static String createConfKey(String dbName, String tblName) {
    return createConfKey(dbName + "." + tblName);
  }

  private static String createConfKey(String fullName) {
    return VALID_WRITEIDS_PREFIX + fullName;
  }

  private ValidWriteIds(String src) {
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
    } else {
      areIdsValid = false;
      ids = null;
    }
  }

  public void addToConf(Configuration conf, String dbName, String tblName) {
    if (source == null) {
      source = toString();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Setting " + createConfKey(dbName, tblName) + " => " + source);
    }
    conf.set(createConfKey(dbName, tblName), source);
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

  public boolean isValidInput(Path file) {
    String fileName = file.getName();
    String[] parts = fileName.split("_", 3);
    if (parts.length < 2 || !MM_PREFIX.equals(parts[0])) {
      LOG.info("Ignoring unknown file for a MM table: " + file
          + " (" + Arrays.toString(parts) + ")");
      return false;
    }
    long writeId = -1;
    try {
      writeId = Long.parseLong(parts[1]);
    } catch (NumberFormatException ex) {
      LOG.info("Ignoring unknown file for a MM table: " + file
          + "; parsing " + parts[1] + " got " + ex.getMessage());
      return false;
    }
    return isValid(writeId);
  }

  public static String getMmFilePrefix(long mmWriteId) {
    return MM_PREFIX + "_" + mmWriteId;
  }


  public static class IdPathFilter implements PathFilter {
    private final String prefix, tmpPrefix;
    private final boolean isMatch;
    public IdPathFilter(long writeId, boolean isMatch) {
      this.prefix = ValidWriteIds.getMmFilePrefix(writeId);
      this.tmpPrefix = "_tmp." + prefix;
      this.isMatch = isMatch;
    }

    @Override
    public boolean accept(Path path) {
      String name = path.getName();
      return isMatch == (name.startsWith(prefix) || name.startsWith(tmpPrefix));
    }
  }
}