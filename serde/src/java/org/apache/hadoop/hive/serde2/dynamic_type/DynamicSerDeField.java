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

package org.apache.hadoop.hive.serde2.dynamic_type;

/**
 * DynamicSerDeField.
 *
 */
public class DynamicSerDeField extends DynamicSerDeSimpleNode {

  // production is:
  // [this.fieldid :] Requiredness() FieldType() this.name FieldValue()
  // [CommaOrSemicolon()]

  private static final int FD_REQUIREDNESS = 0;
  private static final int FD_FIELD_TYPE = 1;

  public boolean isSkippable() {
    return ((DynamicSerDeFieldRequiredness) jjtGetChild(FD_REQUIREDNESS))
        .getRequiredness() == DynamicSerDeFieldRequiredness.RequirednessTypes.Skippable;
  }

  public DynamicSerDeFieldType getFieldType() {
    return (DynamicSerDeFieldType) jjtGetChild(FD_FIELD_TYPE);
  }

  public DynamicSerDeField(int i) {
    super(i);
  }

  public DynamicSerDeField(thrift_grammar p, int i) {
    super(p, i);
  }

}
