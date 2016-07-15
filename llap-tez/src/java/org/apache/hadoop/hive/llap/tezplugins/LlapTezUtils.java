/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.tezplugins;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.input.MultiMRInput;

@InterfaceAudience.Private
public class LlapTezUtils {
  public static boolean isSourceOfInterest(String inputClassName) {
    // MRInput is not of interest since it'll always be ready.
    return !(inputClassName.equals(MRInputLegacy.class.getName()) || inputClassName.equals(
        MultiMRInput.class.getName()) || inputClassName.equals(MRInput.class.getName()));
  }

  public static String getDagId(final JobConf job) {
    return job.get(MRInput.TEZ_MAPREDUCE_DAG_ID);
  }

  public static String getFragmentId(final JobConf job) {
    String taskAttemptId = job.get(MRInput.TEZ_MAPREDUCE_TASK_ATTEMPT_ID);
    if (taskAttemptId != null) {
      return stripAttemptPrefix(taskAttemptId);
    }
    return null;
  }

  public static String stripAttemptPrefix(final String s) {
    if (s.startsWith(TezTaskAttemptID.ATTEMPT)) {
      return s.substring(TezTaskAttemptID.ATTEMPT.length() + 1);
    }
    return s;
  }
}
