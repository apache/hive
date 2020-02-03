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

package org.apache.hadoop.hive.ql.lib;

import java.util.Collection;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Interface for operator graph walker.
 */
public interface SemanticGraphWalker extends GraphWalker {

  /**
   * starting point for walking.
   * 
   * @param startNodes
   *          list of starting operators
   * @param nodeOutput
   *          If this parameter is not null, the call to the function returns
   *          the map from node to objects returned by the processors.
   * @throws SemanticException
   */
  @Override
  void startWalking(Collection<Node> startNodes,
      HashMap<Node, Object> nodeOutput) throws SemanticException;

}
