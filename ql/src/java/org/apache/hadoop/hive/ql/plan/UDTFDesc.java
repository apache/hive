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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

/**
 * All member variables should have a setters and getters of the form get<member
 * name> and set<member name> or else they won't be recreated properly at run
 * time.
 * 
 */
@Explain(displayName = "UDTF Operator")
public class UDTFDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private GenericUDTF genericUDTF;

  public UDTFDesc() {
  }

  public UDTFDesc(final GenericUDTF genericUDTF) {
    this.genericUDTF = genericUDTF;
  }

  public GenericUDTF getGenericUDTF() {
    return genericUDTF;
  }

  public void setGenericUDTF(final GenericUDTF genericUDTF) {
    this.genericUDTF = genericUDTF;
  }

  @Explain(displayName = "function name")
  public String getUDTFName() {
    return genericUDTF.toString();
  }
}
