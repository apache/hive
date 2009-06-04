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

import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import java.util.List;
import java.util.Map;

/**
 * Map Join operator Descriptor implementation.
 * 
 */
@explain(displayName="Common Join Operator")
public class mapJoinDesc extends joinDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private Map<Byte, List<exprNodeDesc>> keys;
  private tableDesc keyTblDesc;
  private List<tableDesc> valueTblDescs;
  
  private int posBigTable;
  
  public mapJoinDesc() { }

  public mapJoinDesc(final Map<Byte, List<exprNodeDesc>> keys, 
                     final tableDesc keyTblDesc, 
                     final Map<Byte, List<exprNodeDesc>> values,
                     final List<tableDesc> valueTblDescs,
                     final int posBigTable,
                     final joinCond[] conds) {
    super(values, conds);
    this.keys        = keys;
    this.keyTblDesc  = keyTblDesc;
    this.valueTblDescs = valueTblDescs;
    this.posBigTable = posBigTable;
  }

  /**
   * @return the keys
   */
  @explain(displayName="keys")
  public Map<Byte, List<exprNodeDesc>> getKeys() {
    return keys;
  }

  /**
   * @param keys the keys to set
   */
  public void setKeys(Map<Byte, List<exprNodeDesc>> keys) {
    this.keys = keys;
  }

  /**
   * @return the position of the big table not in memory
   */
  @explain(displayName="Position of Big Table")
  public int getPosBigTable() {
    return posBigTable;
  }

  /**
   * @param posBigTable the position of the big table not in memory
   */
  public void setPosBigTable(int posBigTable) {
    this.posBigTable = posBigTable;
  }

  /**
   * @return the keyTblDesc
   */
  public tableDesc getKeyTblDesc() {
    return keyTblDesc;
  }

  /**
   * @param keyTblDesc the keyTblDesc to set
   */
  public void setKeyTblDesc(tableDesc keyTblDesc) {
    this.keyTblDesc = keyTblDesc;
  }

  /**
   * @return the valueTblDescs
   */
  public List<tableDesc> getValueTblDescs() {
    return valueTblDescs;
  }

  /**
   * @param valueTblDescs the valueTblDescs to set
   */
  public void setValueTblDescs(List<tableDesc> valueTblDescs) {
    this.valueTblDescs = valueTblDescs;
  }
}
