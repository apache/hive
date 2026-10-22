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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io;

/**
 * A record reader that writes the virtual columns of the rows it returns into an IOContext.
 */
public interface IOContextAwareRecordReader {

  /**
   * Sets the IOContext the virtual columns of each row this reader returns are written into. The
   * HiveContextAwareRecordReader wrapping this reader, directly or through another reader, calls it when it binds
   * its IOContext, before the first row.
   */
  void setIOContext(IOContext ioContext);
}
