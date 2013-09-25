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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.hcatalog.mapreduce;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;

/**
 *  This class will contain an implementation of an OutputCommitter.
 *  See {@link OutputFormatContainer} for more information about containers.
 */
abstract class OutputCommitterContainer extends OutputCommitter {
  private final org.apache.hadoop.mapred.OutputCommitter committer;

  /**
   * @param context current JobContext
   * @param committer OutputCommitter that this instance will contain
   */
  public OutputCommitterContainer(JobContext context, org.apache.hadoop.mapred.OutputCommitter committer) {
    this.committer = committer;
  }

  /**
   * @return underlying OutputCommitter
   */
  public OutputCommitter getBaseOutputCommitter() {
    return committer;
  }

}
