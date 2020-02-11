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

package org.apache.hive.druid;

import java.io.Closeable;
import java.io.IOException;

public abstract class DruidNode implements Closeable{

  private final String nodeType;

  public DruidNode(String nodeId) {this.nodeType = nodeId;}

  final public String getNodeType() {
    return nodeType;
  }

  /**
   * starts the druid node
   */
  public abstract void start() throws IOException;

  /**
   * @return true if the process is working
   */
  public abstract boolean isAlive();

}
