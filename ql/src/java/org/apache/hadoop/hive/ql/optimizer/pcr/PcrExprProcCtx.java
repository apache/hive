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

package org.apache.hadoop.hive.ql.optimizer.pcr;

import java.util.List;

import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;

/**
 * The processor context for partition condition remover. This contains
 * partition pruned for the table scan and table alias.
 */
public class PcrExprProcCtx implements NodeProcessorCtx {

  /**
   * The table alias that is being currently processed.
   */
  private final String tabAlias;
  private final List<Partition> partList;
  private final List<VirtualColumn> vcs;

  public PcrExprProcCtx(String tabAlias, List<Partition> partList) {
    this(tabAlias, partList, null);
  }

  public PcrExprProcCtx(String tabAlias, List<Partition> partList, List<VirtualColumn> vcs) {
    super();
    this.tabAlias = tabAlias;
    this.partList = partList;
    this.vcs = vcs;
  }

  public String getTabAlias() {
    return tabAlias;
  }

  public List<Partition> getPartList() {
    return partList;
  }

  public List<VirtualColumn> getVirtualColumns() {
    return vcs;
  }
}
