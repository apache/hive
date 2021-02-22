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

package org.apache.hadoop.hive.impala.funcmapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import com.google.gson.reflect.TypeToken;
import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.impala.analysis.HdfsUri;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TFunctionBinaryType;
import org.apache.impala.thrift.TPrimitiveType;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * ImpalaBuiltinsDb class.  This class will serve as the replacement INSTANCE on the Hive
 * side for the BuiltinsDb on the Impala side.  The Impala side will instantiate the INSTANCE
 * by calling back to the ImpalaBuiltinsDbLoader in this class.
 */
public class ImpalaBuiltinsDb extends Db {

  private static final String BUILTINS_DB_COMMENT = "System database for Impala builtin functions";

  public static final ImpalaBuiltinsDbLoader LOADER = new ImpalaBuiltinsDbLoader();

  public ImpalaBuiltinsDb() {
    super (BuiltinsDb.NAME, new org.apache.hadoop.hive.metastore.api.Database(BuiltinsDb.NAME,
        BUILTINS_DB_COMMENT, "", Collections.<String,String>emptyMap()));
    setIsSystemDb(true);
    try {
      for (ScalarFunctionDetails functionDetails : ScalarFunctionDetails.getAllFuncDetails()) {
        addFunction(ImpalaFunctionUtil.create(functionDetails));
      }
      for (AggFunctionDetails functionDetails : AggFunctionDetails.getAllFuncDetails()) {
        addFunction(ImpalaFunctionUtil.create(functionDetails));
      }
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

  public static Db getInstance() {
    return BuiltinsDb.getInstance(LOADER);
  }

  private static class ImpalaBuiltinsDbLoader implements BuiltinsDb.BuiltinsDbLoader {
    public Db getBuiltinsDbInstance() {
      return new ImpalaBuiltinsDb();
    }
  }
}
