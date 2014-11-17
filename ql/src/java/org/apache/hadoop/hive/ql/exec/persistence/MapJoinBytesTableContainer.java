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

package org.apache.hadoop.hive.ql.exec.persistence;


import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.WriteBuffers;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Writable;

/**
 * Table container that serializes keys and values using LazyBinarySerDe into
 * BytesBytesMultiHashMap, with very low memory overhead. However,
 * there may be some perf overhead when retrieving rows.
 */
public class MapJoinBytesTableContainer implements MapJoinTableContainer {
  private static final Log LOG = LogFactory.getLog(MapJoinTableContainer.class);

  private final BytesBytesMultiHashMap hashMap;
  /** The OI used to deserialize values. We never deserialize keys. */
  private LazyBinaryStructObjectInspector internalValueOi;
  /**
   * This is kind of hacky. Currently we get BinarySortableSerDe-serialized keys; we could
   * re-serialize them into LazyBinarySerDe, but instead we just reuse the bytes. However, to
   * compare the large table keys correctly when we do, we need to serialize them with correct
   * ordering. Hence, remember the ordering here; it is null if we do use LazyBinarySerDe.
   */
  private boolean[] sortableSortOrders;
  private KeyValueHelper writeHelper;

  private final List<Object> EMPTY_LIST = new ArrayList<Object>(0);

  public MapJoinBytesTableContainer(Configuration hconf,
      MapJoinObjectSerDeContext valCtx, long keyCount, long memUsage) throws SerDeException {
    this(HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEKEYCOUNTADJUSTMENT),
        HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLETHRESHOLD),
        HiveConf.getFloatVar(hconf, HiveConf.ConfVars.HIVEHASHTABLELOADFACTOR),
        HiveConf.getIntVar(hconf, HiveConf.ConfVars.HIVEHASHTABLEWBSIZE),
        valCtx, keyCount, memUsage);
  }

  private MapJoinBytesTableContainer(float keyCountAdj, int threshold, float loadFactor,
      int wbSize, MapJoinObjectSerDeContext valCtx, long keyCount, long memUsage)
          throws SerDeException {
    int newThreshold = HashMapWrapper.calculateTableSize(
        keyCountAdj, threshold, loadFactor, keyCount);
    hashMap = new BytesBytesMultiHashMap(newThreshold, loadFactor, wbSize, memUsage, threshold);
  }

  private LazyBinaryStructObjectInspector createInternalOi(
      MapJoinObjectSerDeContext valCtx) throws SerDeException {
    // We are going to use LBSerDe to serialize values; create OI for retrieval.
    List<? extends StructField> fields = ((StructObjectInspector)
        valCtx.getSerDe().getObjectInspector()).getAllStructFieldRefs();
    List<String> colNames = new ArrayList<String>(fields.size());
    List<ObjectInspector> colOis = new ArrayList<ObjectInspector>(fields.size());
    for (int i = 0; i < fields.size(); ++i) {
      StructField field = fields.get(i);
      colNames.add(field.getFieldName());
      // It would be nice if OI could return typeInfo...
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(
          field.getFieldObjectInspector().getTypeName());
      colOis.add(LazyBinaryUtils.getLazyBinaryObjectInspectorFromTypeInfo(typeInfo));
    }

    return LazyBinaryObjectInspectorFactory
        .getLazyBinaryStructObjectInspector(colNames, colOis);
  }

  private static interface KeyValueHelper extends BytesBytesMultiHashMap.KvSource {
    void setKeyValue(Writable key, Writable val) throws SerDeException;
  }

  private static class KeyValueWriter implements KeyValueHelper {
    private final SerDe keySerDe, valSerDe;
    private final StructObjectInspector keySoi, valSoi;
    private final List<ObjectInspector> keyOis, valOis;
    private final Object[] keyObjs, valObjs;
    private final boolean hasFilterTag;

    public KeyValueWriter(
        SerDe keySerDe, SerDe valSerDe, boolean hasFilterTag) throws SerDeException {
      this.keySerDe = keySerDe;
      this.valSerDe = valSerDe;
      keySoi = (StructObjectInspector)keySerDe.getObjectInspector();
      valSoi = (StructObjectInspector)valSerDe.getObjectInspector();
      List<? extends StructField> keyFields = keySoi.getAllStructFieldRefs(),
          valFields = valSoi.getAllStructFieldRefs();
      keyOis = new ArrayList<ObjectInspector>(keyFields.size());
      valOis = new ArrayList<ObjectInspector>(valFields.size());
      for (int i = 0; i < keyFields.size(); ++i) {
        keyOis.add(keyFields.get(i).getFieldObjectInspector());
      }
      for (int i = 0; i < valFields.size(); ++i) {
        valOis.add(valFields.get(i).getFieldObjectInspector());
      }
      keyObjs = new Object[keyOis.size()];
      valObjs = new Object[valOis.size()];
      this.hasFilterTag = hasFilterTag;
    }

    @Override
    public void writeKey(RandomAccessOutput dest) throws SerDeException {
      LazyBinarySerDe.serializeStruct(dest, keyObjs, keyOis);
    }

    @Override
    public void writeValue(RandomAccessOutput dest) throws SerDeException {
      LazyBinarySerDe.serializeStruct(dest, valObjs, valOis);
    }

    @Override
    public void setKeyValue(Writable key, Writable val) throws SerDeException {
      Object keyObj = keySerDe.deserialize(key), valObj = valSerDe.deserialize(val);
      List<? extends StructField> keyFields = keySoi.getAllStructFieldRefs(),
          valFields = valSoi.getAllStructFieldRefs();
      for (int i = 0; i < keyFields.size(); ++i) {
        keyObjs[i] = keySoi.getStructFieldData(keyObj, keyFields.get(i));
      }
      for (int i = 0; i < valFields.size(); ++i) {
        valObjs[i] = valSoi.getStructFieldData(valObj, valFields.get(i));
      }
    }

    @Override
    public byte updateStateByte(Byte previousValue) {
      if (!hasFilterTag) return (byte)0xff;
      byte aliasFilter = (previousValue == null) ? (byte)0xff : previousValue.byteValue();
      aliasFilter &= ((ShortWritable)valObjs[valObjs.length - 1]).get();
      return aliasFilter;
    }
  }

  private static class LazyBinaryKvWriter implements KeyValueHelper {
    private final LazyBinaryStruct.SingleFieldGetter filterGetter;
    private Writable key, value;
    private final SerDe keySerDe;
    private Boolean hasTag = null; // sanity check - we should not receive keys with tags

    public LazyBinaryKvWriter(SerDe keySerDe, LazyBinaryStructObjectInspector valSoi,
        boolean hasFilterTag) throws SerDeException {
      this.keySerDe = keySerDe;
      if (hasFilterTag) {
        List<? extends StructField> fields = valSoi.getAllStructFieldRefs();
        int ix = fields.size() - 1;
        if (!(fields.get(ix).getFieldObjectInspector() instanceof ShortObjectInspector)) {
          throw new SerDeException("Has filter tag, but corresponding OI is " +
              fields.get(ix).getFieldObjectInspector());
        }
        filterGetter = new LazyBinaryStruct.SingleFieldGetter(valSoi, fields.size() - 1);
      } else {
        filterGetter = null;
      }
    }

    @Override
    public void writeKey(RandomAccessOutput dest) throws SerDeException {
      if (!(key instanceof BinaryComparable)) {
        throw new SerDeException("Unexpected type " + key.getClass().getCanonicalName());
      }
      sanityCheckKeyForTag();
      BinaryComparable b = (BinaryComparable)key;
      dest.write(b.getBytes(), 0, b.getLength() - (hasTag ? 1 : 0));
    }

    /**
     * If we received data with tags from ReduceSinkOperators, no keys will match. This should
     * not happen, but is important enough that we want to find out and work around it if some
     * optimized change causes RSO to pass on tags.
     */
    private void sanityCheckKeyForTag() throws SerDeException {
      if (hasTag != null) return;
      BinaryComparable b = (BinaryComparable)key;
      Object o = keySerDe.deserialize(key);
      StructObjectInspector soi = (StructObjectInspector)keySerDe.getObjectInspector();
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      Object[] data = new Object[fields.size()];
      List<ObjectInspector> fois = new ArrayList<ObjectInspector>(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        data[i] = soi.getStructFieldData(o, fields.get(i));
        fois.add(fields.get(i).getFieldObjectInspector());
      }
      Output output = new Output();
      BinarySortableSerDe.serializeStruct(output, data, fois, new boolean[fields.size()]);
      hasTag = (output.getLength() != b.getLength());
      if (hasTag) {
        LOG.error("Tag found in keys and will be removed. This should not happen.");
        if (output.getLength() != (b.getLength() - 1)) {
          throw new SerDeException(
              "Unexpected tag: " + b.getLength() + " reserialized to " + output.getLength());
        }
      }
    }

    @Override
    public void writeValue(RandomAccessOutput dest) throws SerDeException {
      if (!(value instanceof BinaryComparable)) {
        throw new SerDeException("Unexpected type " + value.getClass().getCanonicalName());
      }
      BinaryComparable b = (BinaryComparable)value;
      dest.write(b.getBytes(), 0, b.getLength());
    }

    @Override
    public void setKeyValue(Writable key, Writable val) {
      this.key = key;
      this.value = val;
    }

    @Override
    public byte updateStateByte(Byte previousValue) {
      if (filterGetter == null) return (byte)0xff;
      byte aliasFilter = (previousValue == null) ? (byte)0xff : previousValue.byteValue();
      filterGetter.init((BinaryComparable)value);
      aliasFilter &= filterGetter.getShort();
      return aliasFilter;
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public MapJoinKey putRow(MapJoinObjectSerDeContext keyContext, Writable currentKey,
      MapJoinObjectSerDeContext valueContext, Writable currentValue) throws SerDeException {
    SerDe keySerde = keyContext.getSerDe(), valSerde = valueContext.getSerDe();
    if (writeHelper == null) {
      LOG.info("Initializing container with "
          + keySerde.getClass().getName() + " and " + valSerde.getClass().getName());
      if (keySerde instanceof BinarySortableSerDe && valSerde instanceof LazyBinarySerDe) {
        LazyBinaryStructObjectInspector valSoi =
            (LazyBinaryStructObjectInspector)valSerde.getObjectInspector();
        writeHelper = new LazyBinaryKvWriter(keySerde, valSoi, valueContext.hasFilterTag());
        internalValueOi = valSoi;
        sortableSortOrders = ((BinarySortableSerDe)keySerde).getSortOrders();
      } else {
        writeHelper = new KeyValueWriter(keySerde, valSerde, valueContext.hasFilterTag());
        internalValueOi = createInternalOi(valueContext);
        sortableSortOrders = null;
      }
    }
    writeHelper.setKeyValue(currentKey, currentValue);
    hashMap.put(writeHelper);
    return null; // there's no key to return
  }

  @Override
  public void clear() {
    hashMap.clear();
  }

  @Override
  public MapJoinKey getAnyKey() {
    return null; // This table has no keys.
  }

  @Override
  public ReusableGetAdaptor createGetter(MapJoinKey keyTypeFromLoader) {
    if (keyTypeFromLoader != null) {
      throw new AssertionError("No key expected from loader but got " + keyTypeFromLoader);
    }
    return new GetAdaptor();
  }

  @Override
  public void seal() {
    hashMap.seal();
  }

  /** Implementation of ReusableGetAdaptor that has Output for key serialization; row
   * container is also created once and reused for every row. */
  private class GetAdaptor implements ReusableGetAdaptor {

    private Object[] currentKey;
    private boolean[] nulls;
    private List<ObjectInspector> vectorKeyOIs;

    private final ReusableRowContainer currentValue;
    private final Output output;

    public GetAdaptor() {
      currentValue = new ReusableRowContainer();
      output = new Output();
    }

    @Override
    public void setFromVector(VectorHashKeyWrapper kw,
        VectorExpressionWriter[] keyOutputWriters,
        VectorHashKeyWrapperBatch keyWrapperBatch) throws HiveException {
      if (nulls == null) {
        nulls = new boolean[keyOutputWriters.length];
        currentKey = new Object[keyOutputWriters.length];
        vectorKeyOIs = new ArrayList<ObjectInspector>();
        for (int i = 0; i < keyOutputWriters.length; i++) {
          vectorKeyOIs.add(keyOutputWriters[i].getObjectInspector());
        }
      } else {
        assert nulls.length == keyOutputWriters.length;
      }
      for (int i = 0; i < keyOutputWriters.length; i++) {
        currentKey[i] = keyWrapperBatch.getWritableKeyValue(kw, i, keyOutputWriters[i]);
        nulls[i] = currentKey[i] == null;
      }
      currentValue.setFromOutput(
          MapJoinKey.serializeRow(output, currentKey, vectorKeyOIs, sortableSortOrders));
    }

    @Override
    public void setFromRow(Object row, List<ExprNodeEvaluator> fields,
        List<ObjectInspector> ois) throws HiveException {
      if (nulls == null) {
        nulls = new boolean[fields.size()];
        currentKey = new Object[fields.size()];
      }
      for (int keyIndex = 0; keyIndex < fields.size(); ++keyIndex) {
        currentKey[keyIndex] = fields.get(keyIndex).evaluate(row);
        nulls[keyIndex] = currentKey[keyIndex] == null;
      }
      currentValue.setFromOutput(
          MapJoinKey.serializeRow(output, currentKey, ois, sortableSortOrders));
    }

    @Override
    public void setFromOther(ReusableGetAdaptor other) {
      assert other instanceof GetAdaptor;
      GetAdaptor other2 = (GetAdaptor)other;
      nulls = other2.nulls;
      currentKey = other2.currentKey;
      currentValue.setFromOutput(other2.output);
    }

    @Override
    public boolean hasAnyNulls(int fieldCount, boolean[] nullsafes) {
      if (nulls == null || nulls.length == 0) return false;
      for (int i = 0; i < nulls.length; i++) {
        if (nulls[i] && (nullsafes == null || !nullsafes[i])) {
          return true;
        }
      }
      return false;
    }

    @Override
    public MapJoinRowContainer getCurrentRows() {
      return currentValue.isEmpty() ? null : currentValue;
    }

    @Override
    public Object[] getCurrentKey() {
      return currentKey;
    }
  }

  /** Row container that gets and deserializes the rows on demand from bytes provided. */
  private class ReusableRowContainer
    implements MapJoinRowContainer, AbstractRowContainer.RowIterator<List<Object>> {
    private byte aliasFilter;
    private List<WriteBuffers.ByteSegmentRef> refs;
    private int currentRow;
    /**
     * Sometimes, when container is empty in multi-table mapjoin, we need to add a dummy row.
     * This container does not normally support adding rows; this is for the dummy row.
     */
    private List<Object> dummyRow = null;

    private final ByteArrayRef uselessIndirection; // LBStruct needs ByteArrayRef
    private final LazyBinaryStruct valueStruct;

    public ReusableRowContainer() {
      if (internalValueOi != null) {
        valueStruct = (LazyBinaryStruct)
            LazyBinaryFactory.createLazyBinaryObject(internalValueOi);
      } else {
        valueStruct = null; // No rows?
      }
      uselessIndirection = new ByteArrayRef();
      clearRows();
    }

    public void setFromOutput(Output output) {
      if (refs == null) {
        refs = new ArrayList<WriteBuffers.ByteSegmentRef>(0);
      }
      byte aliasFilter = hashMap.getValueRefs(output.getData(), output.getLength(), refs);
      this.aliasFilter = refs.isEmpty() ? (byte) 0xff : aliasFilter;
      this.dummyRow = null;
    }

    public boolean isEmpty() {
      return refs.isEmpty() && (dummyRow == null);
    }

    // Implementation of row container
    @Override
    public AbstractRowContainer.RowIterator<List<Object>> rowIter() throws HiveException {
      currentRow = -1;
      return this;
    }

    @Override
    public int rowCount() throws HiveException {
      return dummyRow != null ? 1 : refs.size();
    }

    @Override
    public void clearRows() {
      // Doesn't clear underlying hashtable
      if (refs != null) {
        refs.clear();
      }
      dummyRow = null;
      currentRow = -1;
      aliasFilter = (byte) 0xff;
    }

    @Override
    public byte getAliasFilter() throws HiveException {
      return aliasFilter;
    }

    @Override
    public MapJoinRowContainer copy() throws HiveException {
      return this; // Independent of hashtable and can be modified, no need to copy.
    }

    // Implementation of row iterator
    @Override
    public List<Object> first() throws HiveException {
      currentRow = 0;
      return next();
    }


    @Override
    public List<Object> next() throws HiveException {
      if (dummyRow != null) {
        List<Object> result = dummyRow;
        dummyRow = null;
        return result;
      }
      if (currentRow < 0 || refs.size() < currentRow) throw new HiveException("No rows");
      if (refs.size() == currentRow) return null;
      WriteBuffers.ByteSegmentRef ref = refs.get(currentRow++);
      if (ref.getLength() == 0) {
        return EMPTY_LIST; // shortcut, 0 length means no fields
      }
      if (ref.getBytes() == null) {
        hashMap.populateValue(ref);
      }
      uselessIndirection.setData(ref.getBytes());
      valueStruct.init(uselessIndirection, (int)ref.getOffset(), ref.getLength());
      return valueStruct.getFieldsAsList(); // TODO: should we unset bytes after that?
    }

    @Override
    public void addRow(List<Object> t) {
      if (dummyRow != null || !refs.isEmpty()) {
        throw new RuntimeException("Cannot add rows when not empty");
      }
      dummyRow = t;
    }

    // Various unsupported methods.
    @Override
    public void addRow(Object[] value) {
      throw new RuntimeException(this.getClass().getCanonicalName() + " cannot add arrays");
    }
    @Override
    public void write(MapJoinObjectSerDeContext valueContext, ObjectOutputStream out) {
      throw new RuntimeException(this.getClass().getCanonicalName() + " cannot be serialized");
    }
  }

  public static boolean isSupportedKey(ObjectInspector keyOi) {
    List<? extends StructField> keyFields = ((StructObjectInspector)keyOi).getAllStructFieldRefs();
    for (StructField field : keyFields) {
      if (!MapJoinKey.isSupportedField(field.getFieldObjectInspector())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void dumpMetrics() {
    hashMap.debugDumpMetrics();
  }
}
