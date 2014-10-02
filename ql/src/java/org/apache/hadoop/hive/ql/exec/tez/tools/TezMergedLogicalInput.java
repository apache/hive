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
package org.apache.hadoop.hive.ql.exec.tez.tools;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.MergedInputContext;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.Reader;

/**
 * TezMergedLogicalInput is an adapter to make union input look like
 * a single input in tez.
 */
public class TezMergedLogicalInput extends MergedLogicalInput {

  private Map<Input, Boolean> readyInputs = new IdentityHashMap<Input, Boolean>();

  public TezMergedLogicalInput(MergedInputContext context, List<Input> inputs) {
    super(context, inputs);
  }
 
  @Override
  public Reader getReader() throws Exception {
    return new KeyValuesInputMerger(getInputs());
  }

  @Override
  public void setConstituentInputIsReady(Input input) {
    synchronized (this) {
      readyInputs.put(input, Boolean.TRUE);
    }
    if (readyInputs.size() == getInputs().size()) {
      informInputReady();
    }
  }
}
