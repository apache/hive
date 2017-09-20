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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * LoadTableDesc.
 *
 */
public class LoadTableDesc extends LoadDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private boolean replace;
  private DynamicPartitionCtx dpCtx;
  private ListBucketingCtx lbCtx;
  private boolean inheritTableSpecs = true; //For partitions, flag controlling whether the current
                                            //table specs are to be used

  // TODO: the below seems like they should just be combined into partitionDesc
  private org.apache.hadoop.hive.ql.plan.TableDesc table;
  private Map<String, String> partitionSpec; // NOTE: this partitionSpec has to be ordered map

  public LoadTableDesc(final LoadTableDesc o) {
    super(o.getSourcePath(), o.getWriteType());

    this.replace = o.replace;
    this.dpCtx = o.dpCtx;
    this.lbCtx = o.lbCtx;
    this.inheritTableSpecs = o.inheritTableSpecs;
    this.table = o.table;
    this.partitionSpec = o.partitionSpec;
  }

  public LoadTableDesc(final Path sourcePath,
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final Map<String, String> partitionSpec,
      final boolean replace,
      final AcidUtils.Operation writeType) {
    super(sourcePath, writeType);
    init(table, partitionSpec, replace);
  }

  /**
   * For use with non-ACID compliant operations, such as LOAD
   * @param sourcePath
   * @param table
   * @param partitionSpec
   * @param replace
   */
  public LoadTableDesc(final Path sourcePath,
                       final TableDesc table,
                       final Map<String, String> partitionSpec,
                       final boolean replace) {
    this(sourcePath, table, partitionSpec, replace, AcidUtils.Operation.NOT_ACID);
  }

  public LoadTableDesc(final Path sourcePath,
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final Map<String, String> partitionSpec,
      final AcidUtils.Operation writeType) {
    this(sourcePath, table, partitionSpec, true, writeType);
  }

  /**
   * For DDL operations that are not ACID compliant.
   * @param sourcePath
   * @param table
   * @param partitionSpec
   */
  public LoadTableDesc(final Path sourcePath,
                       final org.apache.hadoop.hive.ql.plan.TableDesc table,
                       final Map<String, String> partitionSpec) {
    this(sourcePath, table, partitionSpec, true, AcidUtils.Operation.NOT_ACID);
  }

  public LoadTableDesc(final Path sourcePath,
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final DynamicPartitionCtx dpCtx,
      final AcidUtils.Operation writeType) {
    super(sourcePath, writeType);
    this.dpCtx = dpCtx;
    if (dpCtx != null && dpCtx.getPartSpec() != null && partitionSpec == null) {
      init(table, dpCtx.getPartSpec(), true);
    } else {
      init(table, new LinkedHashMap<String, String>(), true);
    }
  }

  private void init(
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final Map<String, String> partitionSpec,
      final boolean replace) {
    this.table = table;
    this.partitionSpec = partitionSpec;
    this.replace = replace;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public TableDesc getTable() {
    return table;
  }

  public void setTable(final org.apache.hadoop.hive.ql.plan.TableDesc table) {
    this.table = table;
  }

  @Explain(displayName = "partition")
  public Map<String, String> getPartitionSpec() {
    return partitionSpec;
  }

  public void setPartitionSpec(final Map<String, String> partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  @Explain(displayName = "replace")
  public boolean getReplace() {
    return replace;
  }

  public void setReplace(boolean replace) {
    this.replace = replace;
  }

  public DynamicPartitionCtx getDPCtx() {
    return dpCtx;
  }

  public void setDPCtx(final DynamicPartitionCtx dpCtx) {
    this.dpCtx = dpCtx;
  }

  public boolean getInheritTableSpecs() {
    return inheritTableSpecs;
  }

  public void setInheritTableSpecs(boolean inheritTableSpecs) {
    this.inheritTableSpecs = inheritTableSpecs;
  }

  /**
   * @return the lbCtx
   */
  public ListBucketingCtx getLbCtx() {
    return lbCtx;
  }

  /**
   * @param lbCtx the lbCtx to set
   */
  public void setLbCtx(ListBucketingCtx lbCtx) {
    this.lbCtx = lbCtx;
  }

}
