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

package org.apache.hadoop.hive.serde2.thrift;

import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

/**
 * An interface for TProtocols that can write out data in hadoop Text objects
 * (UTF-8 encoded String). This helps a lot with performance because we don't
 * need to do unnecessary UTF-8 decoding and encoding loops.
 */
public interface WriteTextProtocol {

  /**
   * Write Text.
   */
  void writeText(Text text) throws TException;

}
