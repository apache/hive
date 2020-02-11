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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;

public class HashTableDummyOperator extends Operator<HashTableDummyDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Kryo ctor. */
  protected HashTableDummyOperator() {
    super();
  }

  public HashTableDummyOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    TableDesc tbl = this.getConf().getTbl();
    try {
      Deserializer serde = tbl.getDeserializerClass().newInstance();
      SerDeUtils.initializeSerDe(serde, hconf, tbl.getProperties(), null);
      this.outputObjInspector = serde.getObjectInspector();
    } catch (Exception e) {
      LOG.error("Generating output obj inspector from dummy object error", e);
      e.printStackTrace();
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    throw new HiveException();
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
  }

  @Override
  public String getName() {
    return HashTableDummyOperator.getOperatorName();
  }

  static public String getOperatorName() {
    return "HASHTABLEDUMMY";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.HASHTABLEDUMMY;
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj) || (obj instanceof HashTableDummyOperator) && ((HashTableDummyOperator)obj).operatorId.equals(operatorId);
  }

  @Override
  public int hashCode() {
    return operatorId.hashCode();
  }
}
