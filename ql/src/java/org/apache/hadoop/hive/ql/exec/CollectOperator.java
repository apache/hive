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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.CollectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * Buffers rows emitted by other operators.
 **/
public class CollectOperator extends Operator<CollectDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;
  protected transient Queue<Object> rowList;
  protected transient ObjectInspector standardRowInspector;
  transient int maxSize;

  /** Kryo ctor. */
  protected CollectOperator() {
    super();
  }

  public CollectOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    this.rowList = new ArrayDeque<>();
    maxSize = conf.getBufferSize().intValue();
  }

  boolean firstRow = true;

  @Override
  public void process(Object row, int tag) throws HiveException {
    ObjectInspector rowInspector = inputObjInspectors[tag];
    if (firstRow) {
      firstRow = false;
      // Get the standard ObjectInspector of the row
      standardRowInspector = ObjectInspectorUtils
          .getStandardObjectInspector(rowInspector);
    }

    if (rowList.size() < maxSize) {
      // Create a standard copy of the object.
      Object o = ObjectInspectorUtils.copyToStandardObject(row, rowInspector);
      rowList.add(o);
    }
    forward(row, rowInspector);
  }

  public void retrieve(InspectableObject result) {
    assert (result != null);
    if (rowList.isEmpty()) {
      result.o = null;
      result.oi = null;
    } else {
      result.o = rowList.poll();
      result.oi = standardRowInspector;
    }
  }

  @Override
  public OperatorType getType() {
    return null;
  }

  @Override
  public String getName() {
    return CollectOperator.getOperatorName();
  }

  public static String getOperatorName() {
    return "COLLECT";
  }

  @Override
  public boolean logicalEquals(Operator other) {
    return getClass().getName().equals(other.getClass().getName());
  }
}
