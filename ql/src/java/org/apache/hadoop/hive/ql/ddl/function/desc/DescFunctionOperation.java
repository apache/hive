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

package org.apache.hadoop.hive.ql.ddl.function.desc;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FunctionInfo.FunctionResource;

import static org.apache.commons.lang3.StringUtils.join;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hive.common.util.AnnotationUtils;

/**
 * Operation process of describing a function.
 */
public class DescFunctionOperation extends DDLOperation<DescFunctionDesc> {
  public DescFunctionOperation(DDLOperationContext context, DescFunctionDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    try (DataOutputStream outStream = ShowUtils.getOutputStream(desc.getResFile(), context)) {
      String funcName = desc.getName();
      FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(funcName);
      Class<?> funcClass = functionInfo == null ? null : functionInfo.getFunctionClass();
      Description description = funcClass == null ? null : AnnotationUtils.getAnnotation(funcClass, Description.class);

      printBaseInfo(outStream, funcName, funcClass, description);
      outStream.write(Utilities.newLineCode);
      printExtendedInfoIfRequested(outStream, functionInfo, funcClass);
    } catch (IOException e) {
      LOG.warn("describe function: ", e);
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    }

    return 0;
  }

  private void printBaseInfo(DataOutputStream outStream, String funcName, Class<?> funcClass, Description description)
      throws IOException, SemanticException {
    if (funcClass == null) {
      outStream.writeBytes("Function '" + funcName + "' does not exist.");
    } else if (description == null) {
      outStream.writeBytes("There is no documentation for function '" + funcName + "'");
    } else {
      outStream.writeBytes(description.value().replace("_FUNC_", funcName));
      if (desc.isExtended()) {
        Set<String> synonyms = FunctionRegistry.getFunctionSynonyms(funcName);
        if (synonyms.size() > 0) {
          outStream.writeBytes("\nSynonyms: " + join(synonyms, ", "));
        }
        if (description.extended().length() > 0) {
          outStream.writeBytes("\n" + description.extended().replace("_FUNC_", funcName));
        }
      }
    }
  }


  private void printExtendedInfoIfRequested(DataOutputStream outStream, FunctionInfo functionInfo, Class<?> funcClass)
      throws IOException {
    if (!desc.isExtended()) {
      return;
    }

    if (funcClass != null) {
      outStream.writeBytes("Function class:" + funcClass.getName() + "\n");
    }

    if (functionInfo != null) {
      outStream.writeBytes("Function type:" + functionInfo.getFunctionType() + "\n");
      FunctionResource[] resources = functionInfo.getResources();
      if (resources != null) {
        for (FunctionResource resource : resources) {
          outStream.writeBytes("Resource:" + resource.getResourceURI() + "\n");
        }
      }
    }
  }
}
