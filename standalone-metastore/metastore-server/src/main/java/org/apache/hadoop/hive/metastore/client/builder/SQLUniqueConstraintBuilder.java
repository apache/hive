/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for {@link SQLUniqueConstraint}.  Only requires what {@link ConstraintBuilder} requires.
 */
public class SQLUniqueConstraintBuilder extends ConstraintBuilder<SQLUniqueConstraintBuilder> {

  public SQLUniqueConstraintBuilder() {
    super.setChild(this);
  }

  public List<SQLUniqueConstraint> build(Configuration conf) throws MetaException {
    checkBuildable("unique_constraint", conf);
    List<SQLUniqueConstraint> uc = new ArrayList<>(columns.size());
    for (String column : columns) {
      SQLUniqueConstraint c = new SQLUniqueConstraint(catName, dbName, tableName, column, getNextSeq(),
          constraintName, enable, validate, rely);
      uc.add(c);
    }
    return uc;
  }
}
