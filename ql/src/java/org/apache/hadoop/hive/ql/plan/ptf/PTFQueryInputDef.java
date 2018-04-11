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

package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Input definition")
public class PTFQueryInputDef extends PTFInputDef {
  private String destination;
  private PTFQueryInputType type;

  @Explain(displayName = "destination")
  public String getDestination() {
    return destination;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  public PTFQueryInputType getType() {
    return type;
  }

  public void setType(PTFQueryInputType type) {
    this.type = type;
  }

  @Explain(displayName = "type")
  public String getTypeExplain() {
    return type.name();
  }

  @Override
  public PTFInputDef getInput() {
    return null;
  }
}