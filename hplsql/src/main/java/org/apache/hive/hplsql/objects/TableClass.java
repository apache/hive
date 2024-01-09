/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hive.hplsql.objects;

import static org.apache.hive.hplsql.objects.MethodDictionary.__GETITEM__;
import static org.apache.hive.hplsql.objects.MethodDictionary.__SETITEM__;
import static org.apache.hive.hplsql.objects.MethodParams.Arity.BINARY;
import static org.apache.hive.hplsql.objects.MethodParams.Arity.NULLARY;
import static org.apache.hive.hplsql.objects.MethodParams.Arity.UNARY;

import java.util.ArrayList;
import java.util.List;

import org.apache.hive.hplsql.ColumnDefinition;
import org.apache.hive.hplsql.Row;
import org.apache.hive.hplsql.Var;
import org.apache.hive.hplsql.objects.MethodParams.Arity;


public class TableClass implements HplClass {
  private static final MethodDictionary<Table> methodDictionary = new MethodDictionary<>();
  private final String typeName;
  private final List<ColumnDefinition> columns;
  private final boolean rowType;

  private static final String FIRST = "first";
  private static final String LAST = "last";
  private static final String NEXT = "next";
  private static final String PRIOR = "prior";
  private static final String COUNT = "count";
  private static final String EXISTS = "exists";
  private static final String DELETE = "delete";
  private static final List<String> TABLE_ATTRIBUTES_LIST = new ArrayList<>(9);

  static {
    methodDictionary.put(FIRST, (self, args) -> {
      NULLARY.check(FIRST, args);
      return wrap(self.firstKey());
    });
    TABLE_ATTRIBUTES_LIST.add(FIRST);
    methodDictionary.put(LAST, (self, args) -> {
      NULLARY.check(LAST, args);
      return wrap(self.lastKey());
    });
    TABLE_ATTRIBUTES_LIST.add(LAST);
    methodDictionary.put(NEXT, (self, args) -> {
      Long key = new MethodParams(NEXT, args, UNARY).longAt(0);
      return wrap(self.nextKey(key));
    });
    TABLE_ATTRIBUTES_LIST.add(NEXT);
    methodDictionary.put(PRIOR, (self, args) -> {
      Long key = new MethodParams(PRIOR, args, UNARY).longAt(0);
      return wrap(self.priorKey(key));
    });
    TABLE_ATTRIBUTES_LIST.add(PRIOR);
    methodDictionary.put(COUNT, (self, args) -> {
      NULLARY.check(COUNT, args);
      return new Var(Long.valueOf(self.count()));
    });
    TABLE_ATTRIBUTES_LIST.add(COUNT);
    methodDictionary.put(EXISTS, (self, args) -> {
      Long key = new MethodParams(EXISTS, args, UNARY).longAt(0);
      return new Var(self.existsAt(key));
    });
    TABLE_ATTRIBUTES_LIST.add(EXISTS);
    methodDictionary.put(DELETE, (self, args) -> {
      Arity.max(3).check(DELETE, args);
      if (args.isEmpty()) {
        self.removeAll();
      } else if (args.size() == 1) {
        self.removeAt(args.get(0).value);
      } else {
        self.removeFromTo(args.get(0).value, args.get(1).value);
      }
      return null;
    });
    TABLE_ATTRIBUTES_LIST.add(DELETE);
    methodDictionary.put(__GETITEM__, (self, args) -> {
      Long key = new MethodParams(__GETITEM__, args, UNARY).longAt(0);
      Row row = self.at(key);
      if (row == null) {
        return Var.Null;
      }
      if (self.hplClass().rowType()) {
        Var var = new Var();
        var.setType(Var.Type.ROW.name());
        var.setValue(row);
        return var;
      }
      return row.getValue(0);
    });
    TABLE_ATTRIBUTES_LIST.add(__GETITEM__);
    methodDictionary.put(__SETITEM__, (self, args) -> {
      MethodParams params = new MethodParams(__SETITEM__, args, BINARY);
      long key = params.longAt(0);
      if (self.hplClass().rowType()) {
        self.put(key, params.rowAt(1));
      } else { // single column
        Row row = new Row();
        row.addColumn(
                self.hplClass().columns().get(0).columnName(),
                self.hplClass().columns.get(0).columnTypeString(),
                args.get(1));
        self.put(key, row);
      }
      return Var.Null;
    });
    TABLE_ATTRIBUTES_LIST.add(__SETITEM__);
  }

  private static Var wrap(Object result) {
    return result != null ? new Var((Long) result) : Var.Null;
  }

  public TableClass(String typeName, List<ColumnDefinition> columns, boolean rowType) {
    this.typeName = typeName;
    this.columns = columns;
    this.rowType = rowType;
  }

  public String typeName() {
    return typeName;
  }

  public List<ColumnDefinition> columns() {
    return columns;
  }

  @Override
  public Table newInstance() {
    return new Table(this);
  }

  @Override
  public MethodDictionary methodDictionary() {
    return methodDictionary;
  }

  public boolean rowType() {
    return rowType;
  }

  public static boolean isTableAttributeExists(String property) {
    return TABLE_ATTRIBUTES_LIST.contains(property);
  }
}
