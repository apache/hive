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

package org.apache.hadoop.hive.io;

import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.RecordReader;

/**
 * HiveIOExceptionHandler defines an interface that all io exception handler in
 * Hive should implement. Different IO exception handlers can implement
 * different logics based on the exception input into it.
 */
public interface HiveIOExceptionHandler {

  /**
   * process exceptions raised when creating a record reader.
   *
   * @param e
   * @return RecordReader
   */
  public RecordReader<?, ?> handleRecordReaderCreationException(Exception e)
      throws IOException;

  /**
   * process exceptions thrown when calling rr's next
   *
   * @param e
   * @param result
   * @throws IOException
   */
  public void handleRecorReaderNextException(Exception e,
      HiveIOExceptionNextHandleResult result) throws IOException;

}
