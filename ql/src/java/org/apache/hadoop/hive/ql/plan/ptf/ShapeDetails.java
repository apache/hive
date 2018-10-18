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

package org.apache.hadoop.hive.ql.plan.ptf;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.TypeCheckCtx;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class ShapeDetails {
  String serdeClassName;
  Map<String, String> serdeProps;
  List<String> columnNames;
  transient StructObjectInspector OI;
  transient AbstractSerDe serde;
  transient RowResolver rr;
  transient TypeCheckCtx typeCheckCtx;

  public String getSerdeClassName() {
    return serdeClassName;
  }

  public void setSerdeClassName(String serdeClassName) {
    this.serdeClassName = serdeClassName;
  }

  public Map<String, String> getSerdeProps() {
    return serdeProps;
  }

  public void setSerdeProps(Map<String, String> serdeProps) {
    this.serdeProps = serdeProps;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  public StructObjectInspector getOI() {
    return OI;
  }

  public void setOI(StructObjectInspector oI) {
    OI = oI;
  }

  public AbstractSerDe getSerde() {
    return serde;
  }

  public void setSerde(AbstractSerDe serde) {
    this.serde = serde;
  }

  public RowResolver getRr() {
    return rr;
  }

  public void setRr(RowResolver rr) {
    this.rr = rr;
  }

  public TypeCheckCtx getTypeCheckCtx() {
    return typeCheckCtx;
  }

  public void setTypeCheckCtx(TypeCheckCtx typeCheckCtx) {
    this.typeCheckCtx = typeCheckCtx;
  }
}