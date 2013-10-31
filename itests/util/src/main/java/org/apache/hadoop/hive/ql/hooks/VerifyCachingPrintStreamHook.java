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
package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.common.io.CachingPrintStream;
import org.apache.hadoop.hive.ql.hooks.HookContext.HookType;
import org.apache.hadoop.hive.ql.session.SessionState;

// If this is run as a pre or post execution hook, it writes a message to SessionState.err
// (causing it to be cached if a CachingPrintStream is being used).  If it is run as a failure
// hook, it will write what has been cached by the CachingPrintStream to SessionState.out for
// verification.
public class VerifyCachingPrintStreamHook implements ExecuteWithHookContext {

  public void run(HookContext hookContext) {
    SessionState ss = SessionState.get();

    assert(ss.err instanceof CachingPrintStream);

    if (hookContext.getHookType() == HookType.ON_FAILURE_HOOK) {
      assert(ss.err instanceof CachingPrintStream);
      ss.out.println("Begin cached logs.");
      for (String output : ((CachingPrintStream)ss.err).getOutput()) {
        ss.out.println(output);
      }
      ss.out.println("End cached logs.");
    } else {
      ss.err.println("TEST, this should only appear once in the log.");
    }
  }
}
