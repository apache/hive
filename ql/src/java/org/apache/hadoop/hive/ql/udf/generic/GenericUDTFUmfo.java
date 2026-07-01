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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.anon.extract.Extractor;
import org.apache.hadoop.hive.ql.anon.extract.ExtractorFactory;
import org.apache.hadoop.hive.ql.anon.policy.DataErasurePolicy;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_POLICY_DOC;

@Description(
  name = "umfo",
  value = "_FUNC_(<desc>, cols..., 'id_fld') - "
)
public class GenericUDTFUmfo extends GenericUDTF {

  private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFUmfo.class.getName());

  private transient Object[] forwardObjects = null;
  private Extractor extractor;
  private int bPos = -1;
  private int fPos = -1;
  private int mPos = -1;
  private final List<Integer> lstValues = new ArrayList<>();
  private boolean initialized = false;
  private ColumnInternalFormat internalFormat;
  private String idFieldName;

  @Override
  public void configure(MapredContext mapredContext) {
    super.configure(mapredContext);
    Configuration conf = mapredContext.getJobConf();
    String policyDoc = conf.get(ANON_POLICY_DOC);
    LOG.info("policy: " + policyDoc); // WIP
    DataErasurePolicy policy = DataErasurePolicy.fromString(policyDoc);
  }

  @Override
  public StructObjectInspector initialize(final ObjectInspector[] argOIs) throws UDFArgumentException {

    final String colDesc = getColDesc(argOIs[0], 0);
    verify(colDesc);
    idFieldName = getIdFieldName(argOIs[fPos], fPos);

    final List<ObjectInspector> objectInspectorList = new ArrayList<>();
    final List<String> nameList = new ArrayList<>();

    ObjectInspector oiWi = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT);
    objectInspectorList.add(oiWi);
    nameList.add("key");

    AtomicInteger aiValue = new AtomicInteger();
    for (int vIx : lstValues) {
      ObjectInspector oi = argOIs[vIx];
      objectInspectorList.add(oi);
      nameList.add("val_" + aiValue.getAndIncrement());
    }

    forwardObjects = new Object[lstValues.size() + 1]; // forward only V nad F marked input

    return ObjectInspectorFactory.getStandardStructObjectInspector(nameList, objectInspectorList);
  }

  private String getColDesc(final ObjectInspector oi, final int argIx) throws UDFArgumentException {
    if (!(oi instanceof ConstantObjectInspector)) {
      throw new UDFArgumentException("The first argument to UMFO() must be a constant.");
    }
    final Object value = ((ConstantObjectInspector) oi).getWritableConstantValue();
    if (value == null) {
      throw new UDFArgumentException("The first argument of UMFO() must not be null.");
    }
    if (!(value instanceof Text)) {
      throw new UDFArgumentTypeException(argIx, "The first argument to UMFO() must be a constant string (got " + oi.getTypeName() + " instead).");
    }

    return ((Text) value).toString();
  }

  private String getIdFieldName(final ObjectInspector oi, final int argIx) throws UDFArgumentException {
    if (!(oi instanceof ConstantObjectInspector)) {
      throw new UDFArgumentException("The id field name must be a constant.");
    }
    final Object value = ((ConstantObjectInspector) oi).getWritableConstantValue();
    if (value == null) {
      throw new UDFArgumentException("Id field name value cannot be null.");
    }
    if (!(value instanceof Text)) {
      throw new UDFArgumentTypeException(argIx, "Id field name must be a constant string (got " + oi.getTypeName() + " instead).");
    }

    return ((Text) value).toString();
  }

  private void verify(final String colDesc) throws UDFArgumentException {
    if (initialized) {
      return;
    }

    for (int pos = 0; pos < colDesc.length(); pos++) {
      final char c = colDesc.charAt(pos);
      final ConstCode cc = ConstCode.valueOf(String.valueOf(c));
      switch (cc) {
        case B:
          if (bPos != -1) {
            throw new UDFArgumentException("too many Bs detected!");
          }
          bPos = pos;
          break;
        case O:
        case M:
        case F:
          if (cc == ConstCode.M) {
            mPos = pos;
          }
          lstValues.add(pos);
          break;
        case I:
          if (fPos != -1) {
            throw new UDFArgumentException("too many Fs detected!");
          }
          fPos = pos;
          break;
        case j:
        case m:
        case x:
        case p:
        case a: {
          if (extractor == null) {
            extractor = ExtractorFactory.getExtractor(cc);
          }
          break;
        }
        case d:
          break;
        default:
          throw new UDFArgumentException("bad descriptor");
      }
    }
    if (bPos == -1 || fPos == -1 || lstValues.isEmpty()) {
      throw new UDFArgumentException("bad column descriptor");
    }
    initialized = true;
  }

  @Override
  public void process(final Object[] args) throws HiveException {
    final WritableComparable msgId = (WritableComparable) args[mPos];
    final Writable body = (Writable) args[bPos];
    final Set<WritableComparable> set = new HashSet<>();

    /*
     * skip non PII messages // WIP - PII msg ids could be obtained from the policy doc
    if (((IntWritable) msgId).get() != 3) {
      return;
    }
    */

    if (!extractor.containsIdentityField(idFieldName, msgId, body)) {
      return;
    }

    extractor.extractIdentifyFieldValues((Writable) args[fPos], msgId, body, set);

    for (final Writable pid : set) {
      int ix = 1;
      for (int valueIx : lstValues) {
        forwardObjects[ix] = args[valueIx];
        ix++;
      }

      forwardObjects[0] = pid;
      forward(forwardObjects);
    }
  }

  @Override
  public void close() throws HiveException {
  }

  @Override
  public String toString() {
    return "umfo";
  }
}
