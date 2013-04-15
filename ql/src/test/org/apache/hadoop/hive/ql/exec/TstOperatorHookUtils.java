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

import java.util.Collection;
import java.io.Serializable;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class TstOperatorHookUtils {
  public static final String TEST_OPERATOR_HOOK_ENTER = "TEST_OPERATOR_HOOK_ENTER";
  public static final String TEST_OPERATOR_HOOK_EXIT = "TEST_OPERATOR_HOOK_EXIT";

  public static enum TestOperatorHookCounter {
    TEST_OPERATOR_HOOK_ENTER,
    TEST_OPERATOR_HOOK_EXIT,
  }
}
