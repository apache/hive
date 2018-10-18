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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.events;

import org.apache.hadoop.fs.Path;

/**
 * Exposing the FileSystem implementation outside which is what it should NOT do.
 * <p>
 * Since the bootstrap and incremental for functions is handled similarly. There
 * is additional work to make sure we pass the event object from both places.
 *
 * @see org.apache.hadoop.hive.ql.parse.repl.load.message.CreateFunctionHandler.FunctionDescBuilder
 * would be merged here mostly.
 */
public interface FunctionEvent extends BootstrapEvent {
  Path rootDir();
}
