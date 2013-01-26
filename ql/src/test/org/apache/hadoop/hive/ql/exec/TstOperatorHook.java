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
package org.apache.hadoop.hive.ql.exec;
import java.util.HashMap;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TstOperatorHook implements OperatorHook {
  protected transient Log LOG = LogFactory.getLog(this.getClass().getName());
  private long enters=0;
  private long exits=0;
  public void enter(OperatorHookContext opHookContext) {
    enters++;
  }

  public void exit(OperatorHookContext opHookContext) {
    exits++;
  }

  public void close(OperatorHookContext opHookContext) {
    incrCounter(TstOperatorHookUtils.TEST_OPERATOR_HOOK_ENTER, opHookContext, enters);
    incrCounter(TstOperatorHookUtils.TEST_OPERATOR_HOOK_EXIT, opHookContext, exits);
  }

  private void incrCounter(String ctrName, OperatorHookContext opHookContext, long incrVal) {
    TstOperatorHookUtils.TestOperatorHookCounter ctr =
      TstOperatorHookUtils.TestOperatorHookCounter.valueOf(ctrName);
    Operator op = opHookContext.getOperator();
    LOG.info(ctrName);
    op.reporter.incrCounter(ctr, incrVal);
    Long val = op.reporter.getCounter(ctr).getValue();
    LOG.info(ctrName + " " + String.valueOf(val));
  }
}

