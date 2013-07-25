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

import java.util.List;
import java.util.Map;


/**
 * Demux operator descriptor implementation.
 *
 */
@Explain(displayName = "Demux Operator")
public class DemuxDesc extends AbstractOperatorDesc {

  private static final long serialVersionUID = 1L;

  private Map<Integer, Integer> newTagToOldTag;
  private Map<Integer, Integer> newTagToChildIndex;
  private List<TableDesc> keysSerializeInfos;
  private List<TableDesc> valuesSerializeInfos;
  private Map<Integer, Integer> childIndexToOriginalNumParents;

  public DemuxDesc() {
  }

  public DemuxDesc(
      Map<Integer, Integer> newTagToOldTag,
      Map<Integer, Integer> newTagToChildIndex,
      Map<Integer, Integer> childIndexToOriginalNumParents,
      List<TableDesc> keysSerializeInfos,
      List<TableDesc> valuesSerializeInfos){
    this.newTagToOldTag = newTagToOldTag;
    this.newTagToChildIndex = newTagToChildIndex;
    this.childIndexToOriginalNumParents = childIndexToOriginalNumParents;
    this.keysSerializeInfos = keysSerializeInfos;
    this.valuesSerializeInfos = valuesSerializeInfos;
  }

  public List<TableDesc> getKeysSerializeInfos() {
    return keysSerializeInfos;
  }

  public void setKeysSerializeInfos(List<TableDesc> keysSerializeInfos) {
    this.keysSerializeInfos = keysSerializeInfos;
  }

  public List<TableDesc> getValuesSerializeInfos() {
    return valuesSerializeInfos;
  }

  public void setValuesSerializeInfos(List<TableDesc> valuesSerializeInfos) {
    this.valuesSerializeInfos = valuesSerializeInfos;
  }

  public Map<Integer, Integer> getNewTagToOldTag() {
    return newTagToOldTag;
  }

  public void setNewTagToOldTag(Map<Integer, Integer> newTagToOldTag) {
    this.newTagToOldTag = newTagToOldTag;
  }

  public Map<Integer, Integer> getNewTagToChildIndex() {
    return newTagToChildIndex;
  }

  public void setNewTagToChildIndex(Map<Integer, Integer> newTagToChildIndex) {
    this.newTagToChildIndex = newTagToChildIndex;
  }

  public Map<Integer, Integer> getChildIndexToOriginalNumParents() {
    return childIndexToOriginalNumParents;
  }

  public void setChildIndexToOriginalNumParents(
      Map<Integer, Integer> childIndexToOriginalNumParents) {
    this.childIndexToOriginalNumParents = childIndexToOriginalNumParents;
  }
}
