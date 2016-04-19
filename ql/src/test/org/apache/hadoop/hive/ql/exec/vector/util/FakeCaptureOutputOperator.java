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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;

/**
 * Operator that captures output emitted by parent.
 * Used in unit test only.
 */
public class FakeCaptureOutputOperator extends Operator<FakeCaptureOutputDesc>
  implements Serializable {
  private static final long serialVersionUID = 1L;

  public interface OutputInspector {
    public void inspectRow(Object row, int tag) throws HiveException;
  }

  private OutputInspector outputInspector;

  public void setOutputInspector(OutputInspector outputInspector) {
    this.outputInspector = outputInspector;
  }

  public OutputInspector getOutputInspector() {
    return outputInspector;
  }

  private transient List<Object> rows;

  public static FakeCaptureOutputOperator addCaptureOutputChild(CompilationOpContext ctx,
      Operator<? extends OperatorDesc> op) {
    FakeCaptureOutputOperator out = new FakeCaptureOutputOperator(ctx);
    List<Operator<? extends OperatorDesc>> listParents =
        new ArrayList<Operator<? extends OperatorDesc>>(1);
    listParents.add(op);
    out.setParentOperators(listParents);
    List<Operator<? extends OperatorDesc>> listChildren =
        new ArrayList<Operator<? extends OperatorDesc>>(1);
    listChildren.add(out);
    op.setChildOperators(listChildren);
    return out;
  }


  public List<Object> getCapturedRows() {
    return rows;
  }

  /** Kryo ctor. */
  protected FakeCaptureOutputOperator() {
    super();
  }

  public FakeCaptureOutputOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public void initializeOp(Configuration conf) throws HiveException {
    super.initializeOp(conf);
    rows = new ArrayList<Object>();
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    rows.add(row);
    if (null != outputInspector) {
      outputInspector.inspectRow(row, tag);
    }
  }

  @Override
  public OperatorType getType() {
    return null;
  }

  @Override
  public String getName() {
    return FakeCaptureOutputOperator.getOperatorName();
  }

  public static String getOperatorName() {
    return "FAKE_CAPTURE";
  }
}
