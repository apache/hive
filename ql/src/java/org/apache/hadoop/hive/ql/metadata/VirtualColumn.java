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

package org.apache.hadoop.hive.ql.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

@InterfaceAudience.Private
public class VirtualColumn implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final VirtualColumn FILENAME = new VirtualColumn("INPUT__FILE__NAME", (PrimitiveTypeInfo)TypeInfoFactory.stringTypeInfo);
  public static final VirtualColumn BLOCKOFFSET = new VirtualColumn("BLOCK__OFFSET__INSIDE__FILE", (PrimitiveTypeInfo)TypeInfoFactory.longTypeInfo);
  public static final VirtualColumn ROWOFFSET = new VirtualColumn("ROW__OFFSET__INSIDE__BLOCK", (PrimitiveTypeInfo)TypeInfoFactory.longTypeInfo);

  public static final VirtualColumn RAWDATASIZE = new VirtualColumn("RAW__DATA__SIZE", (PrimitiveTypeInfo)TypeInfoFactory.longTypeInfo);
  /**
   * {@link org.apache.hadoop.hive.ql.io.RecordIdentifier} 
   */
  public static final VirtualColumn ROWID = new VirtualColumn("ROW__ID", RecordIdentifier.StructInfo.typeInfo, true, RecordIdentifier.StructInfo.oi);

  /**
   * GROUPINGID is used with GROUP BY GROUPINGS SETS, ROLLUP and CUBE.
   * It composes a bit vector with the "0" and "1" values for every
   * column which is GROUP BY section. "1" is for a row in the result
   * set if that column has been aggregated in that row. Otherwise the
   * value is "0".  Returns the decimal representation of the bit vector.
   */
  public static final VirtualColumn GROUPINGID =
      new VirtualColumn("GROUPING__ID", (PrimitiveTypeInfo) TypeInfoFactory.intTypeInfo);

  public static ImmutableSet<String> VIRTUAL_COLUMN_NAMES =
      ImmutableSet.of(FILENAME.getName(), BLOCKOFFSET.getName(), ROWOFFSET.getName(),
          RAWDATASIZE.getName(), GROUPINGID.getName(), ROWID.getName());

  private final String name;
  private final TypeInfo typeInfo;
  private final boolean isHidden;
  private final ObjectInspector oi;

  private VirtualColumn(String name, PrimitiveTypeInfo typeInfo) {
    this(name, typeInfo, true, 
      PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo));
  }

  private VirtualColumn(String name, TypeInfo typeInfo, boolean isHidden, ObjectInspector oi) {
    this.name = name;
    this.typeInfo = typeInfo;
    this.isHidden = isHidden;
    this.oi = oi;
  }

  public static List<VirtualColumn> getStatsRegistry(Configuration conf) {
    List<VirtualColumn> l = new ArrayList<VirtualColumn>();
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_RAWDATASIZE)) {
      l.add(RAWDATASIZE);
    }
    return l;
  }

  public static List<VirtualColumn> getRegistry(Configuration conf) {
    ArrayList<VirtualColumn> l = new ArrayList<VirtualColumn>();
    l.add(BLOCKOFFSET);
    l.add(FILENAME);
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEROWOFFSET)) {
      l.add(ROWOFFSET);
    }
    l.add(ROWID);

    return l;
  }

  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

  public String getName() {
    return this.name;
  }

  public boolean isHidden() {
    return isHidden;
  }

  public boolean getIsHidden() {
    return isHidden;
  }

  public ObjectInspector getObjectInspector() {
    return oi;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if(!(o instanceof VirtualColumn)) {
      return false;
    }
    VirtualColumn c = (VirtualColumn) o;
    return this.name.equals(c.name)
        && this.typeInfo.getTypeName().equals(c.getTypeInfo().getTypeName());
  }
  @Override
  public int hashCode() {
    int c = 19;
    c = 31 * name.hashCode() + c;
    return  31 * typeInfo.getTypeName().hashCode() + c;
  }
  public static Collection<String> removeVirtualColumns(final Collection<String> columns) {
    Iterables.removeAll(columns, VIRTUAL_COLUMN_NAMES);
    return columns;
  }

  public static List<TypeInfo> removeVirtualColumnTypes(final List<String> columnNames,
      final List<TypeInfo> columnTypes) {
    if (columnNames.size() != columnTypes.size()) {
      throw new IllegalArgumentException("Number of column names in configuration " +
          columnNames.size() + " differs from column types " + columnTypes.size());
    }

    int i = 0;
    ListIterator<TypeInfo> it = columnTypes.listIterator();
    while(it.hasNext()) {
      it.next();
      if (VIRTUAL_COLUMN_NAMES.contains(columnNames.get(i))) {
        it.remove();
      }
      ++i;
    }
    return columnTypes;
  }

  public static StructObjectInspector getVCSObjectInspector(List<VirtualColumn> vcs) {
    List<String> names = new ArrayList<String>(vcs.size());
    List<ObjectInspector> inspectors = new ArrayList<ObjectInspector>(vcs.size());
    for (VirtualColumn vc : vcs) {
      names.add(vc.getName());
      inspectors.add(vc.oi);
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);
  }

  public static boolean isVirtualColumnBasedOnAlias(ColumnInfo column) {
    // Not using method column.getIsVirtualCol() because partitioning columns
    // are also treated as virtual columns in ColumnInfo.
    if (column.getAlias() != null
        && VirtualColumn.VIRTUAL_COLUMN_NAMES.contains(column.getAlias().toUpperCase())) {
      return true;
    }
    return false;
  }
}
