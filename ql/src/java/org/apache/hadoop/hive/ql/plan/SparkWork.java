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

package org.apache.hadoop.hive.ql.plan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class encapsulates all the work objects that can be executed
 * in a single Spark job. Currently it's basically a tree with MapWork at the
 * leaves and and ReduceWork in all other nodes.
 */
@SuppressWarnings("serial")
@Explain(displayName = "Spark")
public class SparkWork extends AbstractOperatorDesc {
  private static transient final Log logger = LogFactory.getLog(SparkWork.class);

  private static int counter;
  private final String name;
  
  private MapWork mapWork;
  private ReduceWork redWork;

  public SparkWork(String name) {
    this.name = name + ":" + (++counter);
  }

  @Explain(displayName = "DagName")
  public String getName() {
    return name;
  }

  public MapWork getMapWork() {
    return mapWork;
  }

  public void setMapWork(MapWork mapWork) {
    this.mapWork = mapWork;
  }

  public void setReduceWork(ReduceWork redWork) {
    this.redWork = redWork;
  }
  
  public ReduceWork getReduceWork() {
    return redWork;
  }

}
