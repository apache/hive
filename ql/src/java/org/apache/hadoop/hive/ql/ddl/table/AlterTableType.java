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

package org.apache.hadoop.hive.ql.ddl.table;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Enumeration of alter table command types.
 */
public enum AlterTableType {
  // column
  ADDCOLS("add columns"),
  REPLACE_COLUMNS("replace columns"),
  RENAME_COLUMN("rename column"),
  UPDATE_COLUMNS("update columns"),
  // partition
  ADDPARTITION("add partition"),
  DROPPARTITION("drop partition"),
  RENAMEPARTITION("rename partition"), // Note: used in RenamePartitionDesc, not here.
  ALTERPARTITION("alter partition"), // Note: this is never used in AlterTableDesc.
  SETPARTITIONSPEC("set partition spec"),
  EXECUTE("execute"),
  CREATE_BRANCH("create branch"),
  DROP_BRANCH("drop branch"),
  RENAME_BRANCH("rename branch"),
  CREATE_TAG("create tag"),
  DROP_TAG("drop tag"),
  // constraint
  ADD_CONSTRAINT("add constraint"),
  DROP_CONSTRAINT("drop constraint"),
  // storage
  SET_SERDE("set serde"),
  SET_SERDE_PROPS("set serde props"),
  SET_FILE_FORMAT("add fileformat"),
  CLUSTERED_BY("clustered by"),
  NOT_SORTED("not sorted"),
  NOT_CLUSTERED("not clustered"),
  ALTERLOCATION("set location"),
  SKEWED_BY("skewed by"),
  NOT_SKEWED("not skewed"),
  SET_SKEWED_LOCATION("alter skew location"),
  INTO_BUCKETS("alter bucket number"),
  // misc
  ADDPROPS("set properties"),
  DROPPROPS("unset properties"),
  TOUCH("touch"),
  RENAME("rename"),
  OWNER("set owner"),
  ARCHIVE("archive"),
  UNARCHIVE("unarchive"),
  COMPACT("compact"),
  TRUNCATE("truncate"),
  MERGEFILES("merge files"),
  UPDATESTATS("update stats"); // Note: used in ColumnStatsUpdateWork, not here.

  private final String name;

  AlterTableType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public static final List<AlterTableType> NON_NATIVE_TABLE_ALLOWED =
      ImmutableList.of(ADDPROPS, DROPPROPS, ADDCOLS, EXECUTE);

  public static final Set<AlterTableType> SUPPORT_PARTIAL_PARTITION_SPEC =
      ImmutableSet.of(ADDCOLS, REPLACE_COLUMNS, RENAME_COLUMN, ADDPROPS, DROPPROPS, SET_SERDE,
          SET_SERDE_PROPS, SET_FILE_FORMAT);
}
