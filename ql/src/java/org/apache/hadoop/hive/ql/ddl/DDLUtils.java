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

package org.apache.hadoop.hive.ql.ddl;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.hive.common.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utilities used by some DDLOperations.
 */
public final class DDLUtils {
  private static final Logger LOG = LoggerFactory.getLogger("hive.ql.exec.DDLTask");

  private DDLUtils() {
    throw new UnsupportedOperationException("DDLUtils should not be instantiated");
  }

  public static DataOutputStream getOutputStream(Path outputFile, DDLOperationContext context) throws HiveException {
    try {
      FileSystem fs = outputFile.getFileSystem(context.getConf());
      return fs.create(outputFile);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * There are many places where "duplicate" Read/WriteEnity objects are added.  The way this was
   * initially implemented, the duplicate just replaced the previous object.
   * (work.getOutputs() is a Set and WriteEntity#equals() relies on name)
   * This may be benign for ReadEntity and perhaps was benign for WriteEntity before WriteType was
   * added. Now that WriteEntity has a WriteType it replaces it with one with possibly different
   * {@link org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType}. It's hard to imagine
   * how this is desirable.
   *
   * As of HIVE-14993, WriteEntity with different WriteType must be considered different.
   * So WriteEntity created in DDLTask cause extra output in golden files, but only because
   * DDLTask sets a different WriteType for the same Entity.
   *
   * In the spirit of bug-for-bug compatibility, this method ensures we only add new
   * WriteEntity if it's really new.
   *
   * @return {@code true} if item was added
   */
  public static boolean addIfAbsentByName(WriteEntity newWriteEntity, Set<WriteEntity> outputs) {
    for (WriteEntity writeEntity : outputs) {
      if (writeEntity.getName().equalsIgnoreCase(newWriteEntity.getName())) {
        LOG.debug("Ignoring request to add {} because {} is present", newWriteEntity.toStringDetail(),
            writeEntity.toStringDetail());
        return false;
      }
    }
    outputs.add(newWriteEntity);
    return true;
  }

  public static boolean addIfAbsentByName(WriteEntity newWriteEntity, DDLOperationContext context) {
    return addIfAbsentByName(newWriteEntity, context.getWork().getOutputs());
  }

  /**
   * Check if the given serde is valid.
   */
  public static void validateSerDe(String serdeName, DDLOperationContext context) throws HiveException {
    validateSerDe(serdeName, context.getConf());
  }

  public static void validateSerDe(String serdeName, HiveConf conf) throws HiveException {
    try {
      Deserializer d = ReflectionUtil.newInstance(conf.getClassByName(serdeName).
          asSubclass(Deserializer.class), conf);
      if (d != null) {
        LOG.debug("Found class for {}", serdeName);
      }
    } catch (Exception e) {
      throw new HiveException("Cannot validate serde: " + serdeName, e);
    }
  }

  /**
   * Validate if the given table/partition is eligible for update.
   *
   * @param db Database.
   * @param tableName Table name of format db.table
   * @param partSpec Partition spec for the partition
   * @param replicationSpec Replications specification
   *
   * @return boolean true if allow the operation
   * @throws HiveException
   */
  public static boolean allowOperationInReplicationScope(Hive db, String tableName, Map<String, String> partSpec,
      ReplicationSpec replicationSpec) throws HiveException {
    if ((null == replicationSpec) || (!replicationSpec.isInReplicationScope())) {
      // Always allow the operation if it is not in replication scope.
      return true;
    }
    // If the table/partition exist and is older than the event, then just apply the event else noop.
    Table existingTable = db.getTable(tableName, false);
    if ((existingTable != null) && replicationSpec.allowEventReplacementInto(existingTable.getParameters())) {
      // Table exists and is older than the update. Now, need to ensure if update allowed on the partition.
      if (partSpec != null) {
        Partition existingPtn = db.getPartition(existingTable, partSpec, false);
        return ((existingPtn != null) && replicationSpec.allowEventReplacementInto(existingPtn.getParameters()));
      }

      // Replacement is allowed as the existing table is older than event
      return true;
    }

    // The table is missing either due to drop/rename which follows the operation.
    // Or the existing table is newer than our update. So, don't allow the update.
    return false;
  }

  public static String propertiesToString(Map<String, String> props, Set<String> exclude) {
    if (props.isEmpty()) {
      return "";
    }

    SortedMap<String, String> sortedProperties = new TreeMap<String, String>(props);
    List<String> realProps = new ArrayList<String>();
    for (Map.Entry<String, String> e : sortedProperties.entrySet()) {
      if (e.getValue() != null && (exclude == null || !exclude.contains(e.getKey()))) {
        realProps.add("  '" + e.getKey() + "'='" + HiveStringUtils.escapeHiveCommand(e.getValue()) + "'");
      }
    }
    return StringUtils.join(realProps, ", \n");
  }

  public static void writeToFile(String data, String file, DDLOperationContext context) throws IOException {
    if (StringUtils.isEmpty(data)) {
      return;
    }

    Path resFile = new Path(file);
    FileSystem fs = resFile.getFileSystem(context.getConf());
    try (FSDataOutputStream out = fs.create(resFile);
         OutputStreamWriter writer = new OutputStreamWriter(out, "UTF-8")) {
      writer.write(data);
      writer.write((char) Utilities.newLineCode);
      writer.flush();
    }
  }

  public static void appendNonNull(StringBuilder builder, Object value) {
    appendNonNull(builder, value, false);
  }

  public static void appendNonNull(StringBuilder builder, Object value, boolean firstColumn) {
    if (!firstColumn) {
      builder.append((char)Utilities.tabCode);
    } else if (builder.length() > 0) {
      builder.append((char)Utilities.newLineCode);
    }
    if (value != null) {
      builder.append(value);
    }
  }
}
