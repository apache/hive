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

package org.apache.hive.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class HiveMetaDataResultSet<M> extends HiveBaseResultSet {
  protected final List<M> data;

  @SuppressWarnings("unchecked")
  public HiveMetaDataResultSet(final List<String> columnNames
          , final List<String> columnTypes
          , final List<M> data) throws SQLException {
    if (data!=null) {
      this.data = new ArrayList<M>(data);
    } else {
      this.data =  new ArrayList<M>();
    }
    if (columnNames!=null) {
      this.columnNames = new ArrayList<String>(columnNames);
    } else {
      this.columnNames =  new ArrayList<String>();
    }
    if (columnTypes!=null) {
      this.columnTypes = new ArrayList<String>(columnTypes);
    } else {
      this.columnTypes =  new ArrayList<String>();
    }
  }

  @Override
  public void close() throws SQLException {
  }

}
