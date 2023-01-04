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

package org.apache.hadoop.hive.ql;

import java.io.File;

import org.apache.commons.lang3.StringUtils;

public class QTestRunnerUtils {

  /**
   * Executes a set of query files in sequence.
   *
   * @param qfiles array of input query files containing arbitrary number of hive queries
   * @param qt array of QTestUtils, one per qfile
   * @return true if all queries passed, false otw
   */
  public static boolean queryListRunnerSingleThreaded(File[] qfiles, QTestUtil[] qt)
      throws Exception {
    boolean failed = false;
    qt[0].cleanUp();
    qt[0].createSources();
    for (int i = 0; i < qfiles.length && !failed; i++) {
      qt[i].clearTestSideEffects();
      qt[i].cliInit();
      qt[i].executeClient();
      QTestProcessExecResult result = qt[i].checkCliDriverResults();
      if (result.getReturnCode() != 0) {
        failed = true;
        StringBuilder builder = new StringBuilder();
        builder.append("Test ").append(qfiles[i].getName())
            .append(" results check failed with error code ").append(result.getReturnCode());
        if (StringUtils.isNotEmpty(result.getCapturedOutput())) {
          builder.append(" and diff value ").append(result.getCapturedOutput());
        }
        System.err.println(builder.toString());
        System.err.println(QTestUtil.DEBUG_HINT);
        System.err.flush();
      }
      qt[i].clearPostTestEffects();
    }
    return (!failed);
  }

}
