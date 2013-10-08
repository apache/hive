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
package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * THIS INTERFACE IS UNSTABLE AND SHOULD NOT BE USED BY 3RD PARTY UDFS.
 * Interface to allow passing of parameters to the UDF, before it is initialized.
 * For example, to be able to pass the char length parameters to a char type cast.
 */
public interface SettableUDF {

  /**
   * Add data to UDF prior to initialization.
   * An exception may be thrown if the UDF doesn't know what to do with this data.
   * @param params UDF-specific data to add to the UDF
   */
  void setTypeInfo(TypeInfo typeInfo) throws UDFArgumentException;

  TypeInfo getTypeInfo();

}
