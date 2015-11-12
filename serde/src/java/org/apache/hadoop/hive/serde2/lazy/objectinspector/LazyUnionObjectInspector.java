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

package org.apache.hadoop.hive.serde2.lazy.objectinspector;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyUnion;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParameters;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyObjectInspectorParametersImpl;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * LazyUnionObjectInspector works on union data that is stored in LazyUnion.
 *
 * Always use the {@link LazyObjectInspectorFactory} to create new
 * ObjectInspector objects, instead of directly creating an instance of this
 * class.
 */
public class LazyUnionObjectInspector implements UnionObjectInspector {

  public static final Logger LOG = LoggerFactory
      .getLogger(LazyUnionObjectInspector.class.getName());


  private  List<ObjectInspector> ois;
  private byte separator;
  private LazyObjectInspectorParameters lazyParams;

  protected LazyUnionObjectInspector() {
    super();
  }

  protected LazyUnionObjectInspector(
      List<ObjectInspector> ois, byte separator,
      LazyObjectInspectorParameters lazyParams) {
    init(ois, separator, lazyParams);
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardUnionTypeName(this);
  }

  protected void init(
      List<ObjectInspector> ois, byte separator,
      LazyObjectInspectorParameters lazyParams) {
    this.separator = separator;
    this.lazyParams = lazyParams;
    this.ois = new ArrayList<ObjectInspector>();
    this.ois.addAll(ois);
  }

  @Override
  public final Category getCategory() {
    return Category.UNION;
  }

  public byte getSeparator() {
    return separator;
  }

  public Text getNullSequence() {
    return lazyParams.getNullSequence();
  }

  public boolean isEscaped() {
    return lazyParams.isEscaped();
  }

  public byte getEscapeChar() {
    return lazyParams.getEscapeChar();
  }

  public LazyObjectInspectorParameters getLazyParams() {
    return lazyParams;
  }
  @Override
  public Object getField(Object data) {
    if (data == null) {
      return null;
    }
    return ((LazyUnion) data).getField();
  }

  @Override
  public List<ObjectInspector> getObjectInspectors() {
    return ois;
  }

  @Override
  public byte getTag(Object data) {
    if (data == null) {
      return -1;
    }
    return ((LazyUnion) data).getTag();
  }
}
