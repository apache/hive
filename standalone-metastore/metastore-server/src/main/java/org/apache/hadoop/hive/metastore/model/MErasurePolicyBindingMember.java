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

package org.apache.hadoop.hive.metastore.model;

/** Many-to-many join: one row per (binding, policy) pair. */
public class MErasurePolicyBindingMember {

  private MErasurePolicyBinding binding;
  private MErasurePolicy           policy;
  private int                   ordinal;

  public MErasurePolicyBindingMember() {
  }

  public MErasurePolicyBindingMember(MErasurePolicyBinding binding, MErasurePolicy policy, int ordinal) {
    this.binding = binding;
    this.policy = policy;
    this.ordinal = ordinal;
  }

  public MErasurePolicyBinding getBinding() { return binding; }
  public void setBinding(MErasurePolicyBinding binding) { this.binding = binding; }

  public MErasurePolicy getPolicy() { return policy; }
  public void setPolicy(MErasurePolicy policy) { this.policy = policy; }

  public int getOrdinal() { return ordinal; }
  public void setOrdinal(int ordinal) { this.ordinal = ordinal; }
}
