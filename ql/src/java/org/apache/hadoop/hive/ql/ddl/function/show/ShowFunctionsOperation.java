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

package org.apache.hadoop.hive.ql.ddl.function.show;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;

/**
 * Operation process of showing the functions.
 */
public class ShowFunctionsOperation extends DDLOperation<ShowFunctionsDesc> {
  public ShowFunctionsOperation(DDLOperationContext context, ShowFunctionsDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Set<String> funcs = fetchFunctions();
    return printFunctions(funcs);
  }

  private Set<String> fetchFunctions() {
    Set<String> funcs = null;
    if (desc.getPattern() != null) {
      funcs = FunctionRegistry.getFunctionNamesByLikePattern(desc.getPattern());
      LOG.info("Found {} function(s) using pattern {} matching the SHOW FUNCTIONS statement.", funcs.size(),
          desc.getPattern());
    } else {
      funcs = FunctionRegistry.getFunctionNames();
    }

    return funcs;
  }

  private int printFunctions(Set<String> funcs) throws HiveException {
    try (DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      SortedSet<String> sortedFuncs = new TreeSet<String>(funcs);
      sortedFuncs.removeAll(serdeConstants.PrimitiveTypes);

      for (String func : sortedFuncs) {
        outStream.writeBytes(func);
        outStream.write(Utilities.newLineCode);
      }

      return 0;
    } catch (IOException e) {
      LOG.warn("show function: ", e);
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}
