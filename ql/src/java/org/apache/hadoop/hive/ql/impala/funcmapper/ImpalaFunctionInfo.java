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

package org.apache.hadoop.hive.ql.impala.funcmapper;

import org.apache.hadoop.hive.ql.exec.FunctionInfo;

/**
 * Impala's version of the FunctionInfo class.  Stores state that is passed
 * into the FunctionHelper object.
 */
public class ImpalaFunctionInfo extends FunctionInfo {

  private ImpalaFunctionResolver funcResolver;

  private ImpalaFunctionSignature impalaFunctionSignature;

  public ImpalaFunctionInfo(String funcName) {
    super(funcName, "");
  }

  public void setFunctionResolver(ImpalaFunctionResolver funcResolver) {
    this.funcResolver = funcResolver;
  }

  public ImpalaFunctionResolver getFunctionResolver() {
    return funcResolver;
  }

  public void setImpalaFunctionSignature(ImpalaFunctionSignature ifs) {
    impalaFunctionSignature = ifs;
  }

  public ImpalaFunctionSignature getImpalaFunctionSignature() {
    return impalaFunctionSignature;
  }
}
