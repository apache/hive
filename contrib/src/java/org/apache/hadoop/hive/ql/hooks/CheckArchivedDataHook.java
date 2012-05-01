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

import java.util.Set;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Implementation of a pre execute hook that checks whether
 * a partition is archived or not
 */
public class CheckArchivedDataHook {

  private static final String ARCHIVE_FLAG = "archivedFlag";
  final static String DISABLE_CHECK_ARCHIVAL_HOOK = "fbhive.disable.checkArchival.hook";

  public static class PreExec implements PreExecute {

    public void run(SessionState sess, Set<ReadEntity> inputs,
                    Set<WriteEntity> outputs, UserGroupInformation ugi)
      throws Exception {

      // Did the user explicitly ask to disable the hook
      HiveConf conf = sess.getConf();
      String   disableArch = conf.get(DISABLE_CHECK_ARCHIVAL_HOOK);
      if ((disableArch != null) && (disableArch.compareToIgnoreCase("false") == 0)) {
        return;
      }

      //Go over the input paths and check if they are archived or not
      for(ReadEntity re: inputs) {
        boolean isArchived = false;
        if (re.getParameters() != null) {
          String archF = re.getParameters().get(ARCHIVE_FLAG);
          if (archF != null) {
            isArchived = archF.equals("true");
            if (isArchived)
              throw new Exception("Path: " + re.getLocation().toString() +
                                  " needs to be unarchived.");
          }
        }
      }
    }
  }
}
