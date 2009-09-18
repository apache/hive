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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.ConditionalResolver;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;

/**
 * Conditional Task implementation
 **/

public class ConditionalTask extends Task<ConditionalWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  private List<Task<? extends Serializable>> listTasks;
  private Task<? extends Serializable>       resTask;
  
  private ConditionalResolver resolver;
  private Object              resolverCtx;
  
  public boolean isMapRedTask() {
    for (Task<? extends Serializable> task : listTasks)
      if (task.isMapRedTask())
        return true;
    
    return false;
  }
  
  public boolean hasReduce() {
    for (Task<? extends Serializable> task : listTasks)
      if (task.hasReduce())
        return true;
    
    return false;
  }
  
  public void initialize (HiveConf conf) {
    resTask = listTasks.get(resolver.getTaskId(conf, resolverCtx));
    resTask.initialize(conf);
  }
  
  @Override
  public int execute() {
    return resTask.execute();
  }

  /**
   * @return the resolver
   */
  public ConditionalResolver getResolver() {
    return resolver;
  }

  /**
   * @param resolver the resolver to set
   */
  public void setResolver(ConditionalResolver resolver) {
    this.resolver = resolver;
  }

  /**
   * @return the resolverCtx
   */
  public Object getResolverCtx() {
    return resolverCtx;
  }

  /**
   * @param resolverCtx the resolverCtx to set
   */
  public void setResolverCtx(Object resolverCtx) {
    this.resolverCtx = resolverCtx;
  }

  /**
   * @return the listTasks
   */
  public List<Task<? extends Serializable>> getListTasks() {
    return listTasks;
  }

  /**
   * @param listTasks the listTasks to set
   */
  public void setListTasks(List<Task<? extends Serializable>> listTasks) {
    this.listTasks = listTasks;
  }
}
