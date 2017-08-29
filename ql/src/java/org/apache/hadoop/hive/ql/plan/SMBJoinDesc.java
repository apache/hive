/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


@Explain(displayName = "Sorted Merge Bucket Map Join Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class SMBJoinDesc extends MapJoinDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  private MapredLocalWork localWork;

  // keep a mapping from tag to the fetch operator alias
  private HashMap<Byte, String> tagToAlias;
  private Map<String, DummyStoreOperator> aliasToSink;

  public SMBJoinDesc(MapJoinDesc conf) {
    super(conf);
  }

  public SMBJoinDesc() {
    super();
  }

  public SMBJoinDesc(SMBJoinDesc clone) {
    super(clone);
  }

  public MapredLocalWork getLocalWork() {
    return localWork;
  }

  public void setLocalWork(MapredLocalWork localWork) {
    this.localWork = localWork;
  }

  public HashMap<Byte, String> getTagToAlias() {
    return tagToAlias;
  }

  public void setTagToAlias(HashMap<Byte, String> tagToAlias) {
    this.tagToAlias = tagToAlias;
  }

  public Map<String, DummyStoreOperator> getAliasToSink() {
    return aliasToSink;
  }

  public void setAliasToSink(Map<String, DummyStoreOperator> aliasToSink) {
    this.aliasToSink = aliasToSink;
  }
}
