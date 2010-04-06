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

package org.apache.hadoop.hive.ql.session;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.LineageInfo.DataContainer;
import org.apache.hadoop.hive.ql.optimizer.lineage.LineageCtx.Index;

/**
 * LineageState. Contains all the information used to generate the
 * lineage information for the post execution hooks.
 *
 */
public class LineageState {

  /**
   * Mapping from the directory name to FileSinkOperator. This
   * mapping is generated at the filesink operator creation
   * time and is then later used to created the mapping from
   * movetask to the set of filesink operators.
   */
  private final Map<String, FileSinkOperator> dirToFop;

  /**
   * The lineage context index for this query.
   */
  private Index index;

  /**
   * The lineage info structure that is used to pass the lineage
   * information to the hooks.
   */
  private final LineageInfo linfo;

  /**
   * Constructor.
   */
  public LineageState() {
    dirToFop = new HashMap<String, FileSinkOperator>();
    linfo = new LineageInfo();
  }

  /**
   * Adds a mapping from the load work to the file sink operator.
   *
   * @param dir The directory name.
   * @param fop The file sink operator.
   */
  public void mapDirToFop(String dir, FileSinkOperator fop) {
    dirToFop.put(dir, fop);
  }

  /**
   * Set the lineage information for the associated directory.
   *
   * @param dir The directory containing the query results.
   * @param dc The associated data container.
   * @param cols The list of columns.
   */
  public void setLineage(String dir, DataContainer dc,
      List<FieldSchema> cols) {
    // First lookup the file sink operator from the load work.
    FileSinkOperator fop = dirToFop.get(dir);

    // Go over the associated fields and look up the dependencies
    // by position in the row schema of the filesink operator.
    if (fop == null) {
      return;
    }

    List<ColumnInfo> signature = fop.getSchema().getSignature();
    int i = 0;
    for (FieldSchema fs : cols) {
      linfo.putDependency(dc, fs, index.getDependency(fop, signature.get(i++)));
    }
  }

  /**
   * Gets the lineage information.
   *
   * @return LineageInfo.
   */
  public LineageInfo getLineageInfo() {
    return linfo;
  }

  /**
   * Sets the index for the lineage state.
   *
   * @param index The index derived from lineage context.
   */
  public void setIndex(Index index) {
    this.index = index;
  }
}
