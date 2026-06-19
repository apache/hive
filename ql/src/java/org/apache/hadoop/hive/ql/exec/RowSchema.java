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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * RowSchema Implementation.
 */
public class RowSchema implements Serializable {

  private static final long serialVersionUID = 1L;
  private List<ColumnInfo> signature = new ArrayList<ColumnInfo>();

  public RowSchema() {
  }

  public RowSchema(RowSchema that) {
    this.signature = new ArrayList<>(that.signature);
  }

  public RowSchema(List<ColumnInfo> signature) {
    this.signature = signature;
  }

  public void setSignature(List<ColumnInfo> signature) {
    this.signature = signature;
  }

  public List<ColumnInfo> getSignature() {
    return signature;
  }

  public ColumnInfo getColumnInfo(String internalName) {
    for (ColumnInfo columnInfo: this.signature) {
      if (columnInfo.getInternalName().equals(internalName)) {
        return columnInfo;
      }
    }
    return null;
  }

  public ColumnInfo getColumnInfo(String tabAlias, String alias) {
    for (ColumnInfo columnInfo: this.signature) {
      if (columnInfo.getTabAlias() == null) {
        if (tabAlias == null) {
          if(columnInfo.getAlias() != null &&
                  columnInfo.getAlias().equals(alias)) {
            return columnInfo;
          }
        }
      } else {
        if (tabAlias != null) {
          if (columnInfo.getTabAlias().equals(tabAlias) &&
                  columnInfo.getAlias() != null &&
                  columnInfo.getAlias().equals(alias)) {
            return columnInfo;
          }
        }
      }
    }
    return null;
  }

  public int getPosition(String internalName) {
    int pos = -1;
    for (ColumnInfo var : this.signature) {
      ++pos;
      if (var.getInternalName().equals(internalName)) {
        return pos;
      }
    }
    return -1;
  }

  public Set<String> getTableNames() {
    Set<String> tableNames = new HashSet<String>();
    for (ColumnInfo var : this.signature) {
      tableNames.add(var.getTabAlias());
    }
    return tableNames;
  }

  public List<String> getColumnNames() {
    List<String> columnNames = new ArrayList<String>();
    for (ColumnInfo var : this.signature) {
      columnNames.add(var.getInternalName());
    }
    return columnNames;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RowSchema) || (obj == null)) {
      return false;
    }
    if(this == obj) {
      return true;
    }

    RowSchema dest = (RowSchema)obj;
    if(this.signature == null && dest.getSignature() == null) {
      return true;
    }
    if((this.signature == null && dest.getSignature() != null) ||
        (this.signature != null && dest.getSignature() == null) ) {
      return false;
    }

    if(this.signature.size() != dest.getSignature().size()) {
      return false;
    }

    Iterator<ColumnInfo> origIt = this.signature.iterator();
    Iterator<ColumnInfo> destIt = dest.getSignature().iterator();
    while(origIt.hasNext()) {
      ColumnInfo origColumn = origIt.next();
      ColumnInfo destColumn = destIt.next();

      if(origColumn == null && destColumn == null) {
        continue;
      }

      if((origColumn == null && destColumn != null) ||
          (origColumn != null && destColumn == null) ) {
        return false;
      }

      if(!origColumn.equals(destColumn)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    for (ColumnInfo col: signature) {
      if (sb.length() > 1) {
        sb.append(',');
      }
      sb.append(col.toString());
      sb.append('|');
      sb.append('{');
      sb.append(col.getTabAlias());
      sb.append('}');
      if (col.getAlias() != null) {
        sb.append(col.getAlias());
      }
    }
    sb.append(')');
    return sb.toString();
  }
}
