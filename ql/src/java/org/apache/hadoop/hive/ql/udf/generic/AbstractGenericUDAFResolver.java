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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * An abstract class to help facilitate existing implementations of
 * <tt>GenericUDAFResolver</tt> to migrate towards the newly introduced
 * interface {@link GenericUDAFResolver2}. This class provides a default
 * implementation of this new API and in turn calls
 * the existing API {@link GenericUDAFResolver#getEvaluator(TypeInfo[])} by
 * ignoring the extra parameter information available via the
 * <tt>GenericUDAFParameterInfo</tt> interface.
 *
 */
public abstract class AbstractGenericUDAFResolver
    implements GenericUDAFResolver2
{

  @SuppressWarnings("deprecation")
  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
    throws SemanticException {

    if (info.isAllColumns()) {
      throw new SemanticException(
          "The specified syntax for UDAF invocation is invalid.");
    }

    return getEvaluator(info.getParameters());
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) 
    throws SemanticException {
    throw new SemanticException(
          "This UDAF does not support the deprecated getEvaluator() method.");
  }
}
