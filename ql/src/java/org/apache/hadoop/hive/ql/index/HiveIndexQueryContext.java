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
package org.apache.hadoop.hive.ql.index;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

/**
 * Used to pass information between the IndexProcessor and the plugin
 * IndexHandler during query processing
 *
 */
public class HiveIndexQueryContext {

  private Set<ReadEntity> additionalSemanticInputs; // additional inputs to add to the parse context when
                                                        // merging the index query tasks
  private String indexInputFormat;        // input format to set on the TableScanOperator to activate indexing
  private String indexIntermediateFile;   // name of intermediate file written by the index query for the
                                          // TableScanOperator to use
  private List<Task<? extends Serializable>> queryTasks;      // list of tasks that will execute the index query and write
                                                              // results to a temporary file
  private ExprNodeDesc residualPredicate; // predicate that could not be processed by an index handler
                                          // and should be used on the base table scan (see HIVE-2115)
  private Set<Partition> queryPartitions; // partitions accessed by the original query

  public HiveIndexQueryContext() {
    this.additionalSemanticInputs = null;
    this.indexInputFormat = null;
    this.indexIntermediateFile = null;
    this.queryTasks = null;
  }

  public Set<ReadEntity> getAdditionalSemanticInputs() {
    return additionalSemanticInputs;
  }
  public void addAdditionalSemanticInputs(Set<ReadEntity> additionalParseInputs) {
    if (this.additionalSemanticInputs == null) {
      this.additionalSemanticInputs = new LinkedHashSet<ReadEntity>();
    }
    this.additionalSemanticInputs.addAll(additionalParseInputs);
  }

  public String getIndexInputFormat() {
    return indexInputFormat;
  }
  public void setIndexInputFormat(String indexInputFormat) {
    this.indexInputFormat = indexInputFormat;
  }

  public String getIndexIntermediateFile() {
    return indexIntermediateFile;
  }
  public void setIndexIntermediateFile(String indexIntermediateFile) {
    this.indexIntermediateFile = indexIntermediateFile;
  }

  public List<Task<? extends Serializable>> getQueryTasks() {
    return queryTasks;
  }
  public void setQueryTasks(List<Task<? extends Serializable>> indexQueryTasks) {
    this.queryTasks = indexQueryTasks;
  }

  public void setResidualPredicate(ExprNodeDesc residualPredicate) {
    this.residualPredicate = residualPredicate;
  }

  public ExprNodeDesc getResidualPredicate() {
    return residualPredicate;
  }

  public Set<Partition> getQueryPartitions() {
    return queryPartitions;
  }

  public void setQueryPartitions(Set<Partition> queryPartitions) {
    this.queryPartitions = queryPartitions;
  }
}
