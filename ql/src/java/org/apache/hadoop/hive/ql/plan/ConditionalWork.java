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

import java.io.Serializable;
import java.util.List;

@explain(displayName="Conditional Operator")
public class ConditionalWork implements Serializable {
  private static final long serialVersionUID = 1L;
  List<? extends Serializable> listWorks;
  
  public ConditionalWork() {
  }
  
  public ConditionalWork(final List<? extends Serializable> listWorks) {
    this.listWorks = listWorks;
  }

  /**
   * @return the listWorks
   */
  @explain(displayName="list of dependent Tasks")
  public List<? extends Serializable> getListWorks() {
    return listWorks;
  }

  /**
   * @param listWorks the listWorks to set
   */
  public void setListWorks(List<? extends Serializable> listWorks) {
    this.listWorks = listWorks;
  }
}
