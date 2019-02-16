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

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SerdeType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This collects together SerdeInfo and columns, since StorageDescriptor and SchemaVersion share
 * those traits.
 * @param <T>
 */
abstract class SerdeAndColsBuilder<T> {
  private static final String SERDE_LIB = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";

  private List<FieldSchema> cols;
  private String serdeName, serdeLib, serdeDescription, serdeSerializerClass, serdeDeserializerClass;
  private Map<String, String> serdeParams;
  private SerdeType serdeType;
  protected T child;

  protected SerdeAndColsBuilder() {
    serdeParams = new HashMap<>();
    serdeLib = SERDE_LIB;
  }

  protected void setChild(T child) {
    this.child = child;
  }

  protected SerDeInfo buildSerde() {
    SerDeInfo serDeInfo = new SerDeInfo(serdeName, serdeLib, serdeParams);
    if (serdeDescription != null) serDeInfo.setDescription(serdeDescription);
    if (serdeSerializerClass != null) serDeInfo.setSerializerClass(serdeSerializerClass);
    if (serdeDeserializerClass != null) serDeInfo.setDeserializerClass(serdeDeserializerClass);
    if (serdeType != null) serDeInfo.setSerdeType(serdeType);
    return serDeInfo;
  }

  protected List<FieldSchema> getCols() throws MetaException {
    if (cols == null) throw new MetaException("You must provide the columns");
    return cols;
  }

  public T setCols(
      List<FieldSchema> cols) {
    this.cols = cols;
    return child;
  }

  public T addCol(String name, String type, String comment) {
    if (cols == null) cols = new ArrayList<>();
    cols.add(new FieldSchema(name, type, comment));
    return child;
  }

  public T addCol(String name, String type) {
    return addCol(name, type, "");
  }

  public T setSerdeName(String serdeName) {
    this.serdeName = serdeName;
    return child;
  }

  public T setSerdeLib(String serdeLib) {
    this.serdeLib = serdeLib;
    return child;
  }

  public T setSerdeDescription(String serdeDescription) {
    this.serdeDescription = serdeDescription;
    return child;
  }

  public T setSerdeSerializerClass(String serdeSerializerClass) {
    this.serdeSerializerClass = serdeSerializerClass;
    return child;
  }

  public T setSerdeDeserializerClass(String serdeDeserializerClass) {
    this.serdeDeserializerClass = serdeDeserializerClass;
    return child;
  }

  public T setSerdeParams(
      Map<String, String> serdeParams) {
    this.serdeParams = serdeParams;
    return child;
  }

  public T addSerdeParam(String key, String value) {
    if (serdeParams == null) serdeParams = new HashMap<>();
    serdeParams.put(key, value);
    return child;
  }

  public T setSerdeType(SerdeType serdeType) {
    this.serdeType = serdeType;
    return child;
  }
}
