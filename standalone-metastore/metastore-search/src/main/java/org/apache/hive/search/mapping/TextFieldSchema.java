/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.mapping;

import org.apache.commons.lang3.StringUtils;

public class TextFieldSchema implements FieldSchema {
  private final String name;
  private final boolean lexical;
  private String semanticModel;
  private boolean storeField;
  private boolean filter;

  public TextFieldSchema(String name, boolean lexical) {
    this.name = name;
    this.lexical = lexical;
  }

  public TextFieldSchema storeField(boolean store) {
    this.storeField = store;
    return this;
  }

  public TextFieldSchema filterField(boolean filter) {
    this.filter = filter;
    return this;
  }

  public TextFieldSchema semanticField(String model) {
    this.semanticModel = model;
    return this;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean store() {
    return storeField;
  }

  @Override
  public boolean filter() {
    return filter;
  }

  @Override
  public boolean lexical() {
    return lexical;
  }

  public String semanticModel() {
    return semanticModel;
  }

  public boolean semantic() {
    return StringUtils.isNotEmpty(semanticModel);
  }
}
