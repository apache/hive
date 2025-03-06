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

package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

/**
 * A Hive Table Partition: is a fundamental storage unit within a Table. Currently, Hive does not support
 * hierarchical partitions - For eg: if partition ds=1, hr=1 exists, there is no way to access ds=1
 *
 * Hierarchical partitions are needed in some cases, for eg. locking. For now, create a dummy partition to
 * satisfy this
 */
public class DummyPartition extends Partition {

  private String name;
  private LinkedHashMap<String, String> partSpec;
  public DummyPartition() {
  }
  
  public DummyPartition(Table tbl, String name) {
    setTable(tbl);
    this.name = name;
  }

  public DummyPartition(Table tbl) {
    this(tbl, null, Maps.newHashMap());
  }

  public DummyPartition(Table tbl, String name, Map<String, String> partSpec) {
    this(tbl, name);
    org.apache.hadoop.hive.metastore.api.Partition tPart =
        new org.apache.hadoop.hive.metastore.api.Partition();
    tPart.setSd(tbl.getSd().deepCopy());
    tPart.setParameters(Maps.newHashMap());
    
    this.partSpec = Maps.newLinkedHashMap(partSpec);
    setTPartition(tPart);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getCompleteName() {
    return getName();
  }

  @Override
  public LinkedHashMap<String, String> getSpec() {
    return partSpec;
  }

  @Override
  public List<String> getValues() {
    List<String> values = new ArrayList<String>();
    for (FieldSchema fs : this.getTable().getPartCols()) {
      values.add(partSpec.get(fs.getName()));
    }
    return values;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof DummyPartition) {
      DummyPartition o = (DummyPartition) obj;
      return Objects.equals(name, o.name) && Objects.equals(partSpec, o.partSpec);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name) + Objects.hashCode(partSpec);
  }
}
