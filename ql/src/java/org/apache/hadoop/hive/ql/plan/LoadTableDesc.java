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

/**
 * LoadTableDesc.
 *
 */
public class LoadTableDesc extends org.apache.hadoop.hive.ql.plan.LoadDesc
    implements Serializable {
  private static final long serialVersionUID = 1L;
  private boolean replace;
  private String tmpDir;
  private DynamicPartitionCtx dpCtx;
  private ListBucketingCtx lbCtx;
  private boolean holdDDLTime;
  private boolean inheritTableSpecs = true; //For partitions, flag controlling whether the current
                                            //table specs are to be used

  // TODO: the below seems like they should just be combined into partitionDesc
  private org.apache.hadoop.hive.ql.plan.TableDesc table;
  private Map<String, String> partitionSpec; // NOTE: this partitionSpec has to be ordered map

  public LoadTableDesc() {
    this.holdDDLTime = false;
  }

  public LoadTableDesc(final String sourceDir, final String tmpDir,
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final Map<String, String> partitionSpec, final boolean replace) {
    super(sourceDir);
    init(sourceDir, tmpDir, table, partitionSpec, replace);
  }

  public LoadTableDesc(final String sourceDir, final String tmpDir,
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final Map<String, String> partitionSpec) {
    this(sourceDir, tmpDir, table, partitionSpec, true);
  }

  public LoadTableDesc(final String sourceDir, final String tmpDir,
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final DynamicPartitionCtx dpCtx) {
    super(sourceDir);
    this.dpCtx = dpCtx;
    if (dpCtx != null && dpCtx.getPartSpec() != null && partitionSpec == null) {
      init(sourceDir, tmpDir, table, dpCtx.getPartSpec(), true);
    } else {
      init(sourceDir, tmpDir, table, new LinkedHashMap<String, String>(), true);
    }
  }

  private void init(final String sourceDir, final String tmpDir,
      final org.apache.hadoop.hive.ql.plan.TableDesc table,
      final Map<String, String> partitionSpec, final boolean replace) {
    this.tmpDir = tmpDir;
    this.table = table;
    this.partitionSpec = partitionSpec;
    this.replace = replace;
    this.holdDDLTime = false;
  }

  public void setHoldDDLTime(boolean ddlTime) {
    holdDDLTime = ddlTime;
  }

  public boolean getHoldDDLTime() {
    return holdDDLTime;
  }

  @Explain(displayName = "tmp directory", normalExplain = false)
  public String getTmpDir() {
    return tmpDir;
  }

  public void setTmpDir(final String tmp) {
    tmpDir = tmp;
  }

  @Explain(displayName = "table")
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
