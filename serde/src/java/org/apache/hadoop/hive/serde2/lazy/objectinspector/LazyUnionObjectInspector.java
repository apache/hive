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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyUnion;
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

  public static final Log LOG = LogFactory
      .getLog(LazyUnionObjectInspector.class.getName());


  private  List<ObjectInspector> ois;
  private byte separator;
  private Text nullSequence;
  private boolean escaped;
  private byte escapeChar;

  protected LazyUnionObjectInspector() {
    super();
  }
  protected LazyUnionObjectInspector(
      List<ObjectInspector> ois, byte separator,
      Text nullSequence, boolean escaped,
      byte escapeChar) {
    init(ois, separator,
        nullSequence, escaped, escapeChar);
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardUnionTypeName(this);
  }

  protected void init(
      List<ObjectInspector> ois, byte separator,
      Text nullSequence, boolean escaped,
      byte escapeChar) {
    this.separator = separator;
    this.nullSequence = nullSequence;
    this.escaped = escaped;
    this.escapeChar = escapeChar;
    this.ois = new ArrayList<ObjectInspector>();
    this.ois.addAll(ois);
  }

  protected LazyUnionObjectInspector(List<ObjectInspector> ois,
      byte separator, Text nullSequence) {
    init(ois, separator, nullSequence);
  }

  protected void init(List<ObjectInspector> ois, byte separator,
      Text nullSequence) {
    this.separator = separator;
    this.nullSequence = nullSequence;
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
    return nullSequence;
  }

  public boolean isEscaped() {
    return escaped;
  }

  public byte getEscapeChar() {
    return escapeChar;
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
