/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon;

import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.records.TezTaskAttemptID;

public interface KilledTaskHandler {

  // TODO Ideally, this should only need to send in the TaskAttemptId. Everything else should be
  // inferred from this.
  // Passing in parameters until there's some dag information stored and tracked in the daemon.
  void taskKilled(String amLocation, int port, String user,
                  Token<JobTokenIdentifier> jobToken, String queryId, String dagName,
                  TezTaskAttemptID taskAttemptId);
}
