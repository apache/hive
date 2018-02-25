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

package org.apache.hadoop.hive.ql.plan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.io.DataOutputBuffer;


@SuppressWarnings("serial")
@Explain(displayName = "Application Master Event Operator")
public class AppMasterEventDesc extends AbstractOperatorDesc {

  private TableDesc table;
  private String vertexName;
  private String inputName;

  @Explain(displayName = "Target Vertex")
  public String getVertexName() {
    return vertexName;
  }

  @Explain(displayName = "Target Input")
  public String getInputName() {
    return inputName;
  }

  public void setInputName(String inputName) {
    this.inputName = inputName;
  }

  public void setVertexName(String vertexName) {
    this.vertexName = vertexName;
  }

  public TableDesc getTable() {
    return table;
  }

  public void setTable(TableDesc table) {
    this.table = table;
  }

  public void writeEventHeader(DataOutputBuffer buffer) throws IOException {
    // nothing to add
  }

  public class AppMasterEventOperatorExplainVectorization extends OperatorExplainVectorization {

    private final AppMasterEventDesc appMasterEventDesc;
    private final VectorAppMasterEventDesc vectorAppMasterEventDesc;

    public AppMasterEventOperatorExplainVectorization(AppMasterEventDesc appMasterEventDesc,
        VectorAppMasterEventDesc vectorAppMasterEventDesc) {
      // Native vectorization supported.
      super(vectorAppMasterEventDesc, true);
      this.appMasterEventDesc = appMasterEventDesc;
      this.vectorAppMasterEventDesc = vectorAppMasterEventDesc;
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "App Master Event Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public AppMasterEventOperatorExplainVectorization getAppMasterEventVectorization() {
    VectorAppMasterEventDesc vectorAppMasterEventDesc = (VectorAppMasterEventDesc) getVectorDesc();
    if (vectorAppMasterEventDesc == null) {
      return null;
    }
    return new AppMasterEventOperatorExplainVectorization(this, vectorAppMasterEventDesc);
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (getClass().getName().equals(other.getClass().getName())) {
      AppMasterEventDesc otherDesc = (AppMasterEventDesc) other;
      return Objects.equals(getInputName(), otherDesc.getInputName()) &&
          Objects.equals(getVertexName(), otherDesc.getVertexName()) &&
          Objects.equals(getTable(), otherDesc.getTable());
    }
    return false;
  }
}
