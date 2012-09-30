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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Correlation dispatch operator Descriptor implementation.
 *
 */
@Explain(displayName = "Correlation Dispatch Operator")
public class CorrelationReducerDispatchDesc extends AbstractOperatorDesc {

  private static final long serialVersionUID = 1L;

  private Map<Integer, Map<Integer, List<Integer>>> dispatchConf;
  private Map<Integer, Map<Integer, List<SelectDesc>>> dispatchValueSelectDescConf;
  private Map<Integer, Map<Integer, List<SelectDesc>>> dispatchKeySelectDescConf;

  public CorrelationReducerDispatchDesc(){
    this.dispatchConf = new HashMap<Integer, Map<Integer, List<Integer>>>();
    this.dispatchValueSelectDescConf = new HashMap<Integer, Map<Integer, List<SelectDesc>>>();
    this.dispatchKeySelectDescConf = new HashMap<Integer, Map<Integer, List<SelectDesc>>>();

  }

  public CorrelationReducerDispatchDesc(Map<Integer, Map<Integer, List<Integer>>> dispatchConf){
    this.dispatchConf = dispatchConf;
    this.dispatchValueSelectDescConf = new HashMap<Integer, Map<Integer,List<SelectDesc>>>();
    this.dispatchKeySelectDescConf = new HashMap<Integer, Map<Integer,List<SelectDesc>>>();
    for(Entry<Integer, Map<Integer, List<Integer>>> entry: this.dispatchConf.entrySet()){
      HashMap<Integer, List<SelectDesc>> tmp = new HashMap<Integer, List<SelectDesc>>();
      for(Integer child: entry.getValue().keySet()){
        tmp.put(child, new ArrayList<SelectDesc>());
        tmp.get(child).add(new SelectDesc(true));
      }
      this.dispatchValueSelectDescConf.put(entry.getKey(), tmp);
      this.dispatchKeySelectDescConf.put(entry.getKey(), tmp);
    }
  }

  public CorrelationReducerDispatchDesc(Map<Integer, Map<Integer, List<Integer>>> dispatchConf,
      Map<Integer, Map<Integer, List<SelectDesc>>> dispatchKeySelectDescConf,
      Map<Integer, Map<Integer, List<SelectDesc>>> dispatchValueSelectDescConf){
    this.dispatchConf = dispatchConf;
    this.dispatchValueSelectDescConf = dispatchValueSelectDescConf;
    this.dispatchKeySelectDescConf = dispatchKeySelectDescConf;
  }

  public void setDispatchConf(Map<Integer, Map<Integer, List<Integer>>> dispatchConf){
    this.dispatchConf = dispatchConf;
  }

  public Map<Integer, Map<Integer, List<Integer>>> getDispatchConf(){
    return this.dispatchConf;
  }

  public void setDispatchValueSelectDescConf(Map<Integer, Map<Integer,List<SelectDesc>>> dispatchValueSelectDescConf){
    this.dispatchValueSelectDescConf = dispatchValueSelectDescConf;
  }

  public Map<Integer, Map<Integer,List<SelectDesc>>> getDispatchValueSelectDescConf(){
    return this.dispatchValueSelectDescConf;
  }

  public void setDispatchKeySelectDescConf(Map<Integer, Map<Integer,List<SelectDesc>>> dispatchKeySelectDescConf){
    this.dispatchKeySelectDescConf = dispatchKeySelectDescConf;
  }

  public Map<Integer, Map<Integer, List<SelectDesc>>> getDispatchKeySelectDescConf() {
    return this.dispatchKeySelectDescConf;
  }

}
