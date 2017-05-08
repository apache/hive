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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapper;
import org.apache.hadoop.hive.ql.exec.vector.VectorHashKeyWrapperBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hive.common.util.HashCodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table container that serializes keys and values using LazyBinarySerDe into
 * BytesBytesMultiHashMap, with very low memory overhead. However,
 * there may be some perf overhead when retrieving rows.
 */
public class MapJoinBytesTableContainer
         implements MapJoinTableContainer, MapJoinTableContainerDirectAccess {
  private static final Logger LOG = LoggerFactory.getLogger(MapJoinTableContainer.class);

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
  private byte[] nullMarkers;
  private byte[] notNullMarkers;
  private KeyValueHelper writeHelper;
  private DirectKeyValueWriter directWriteHelper;

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
    hashMap = new BytesBytesMultiHashMap(newThreshold, loadFactor, wbSize, memUsage);
    directWriteHelper = new DirectKeyValueWriter();
  }

  public MapJoinBytesTableContainer(BytesBytesMultiHashMap hashMap) {
    this.hashMap = hashMap;
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

  public void setInternalValueOi(LazyBinaryStructObjectInspector internalValueOi) {
    this.internalValueOi = internalValueOi;
  }

  public void setSortableSortOrders(boolean[] sortableSortOrders) {
    this.sortableSortOrders = sortableSortOrders;
  }

  public void setNullMarkers(byte[] nullMarkers) {
    this.nullMarkers = nullMarkers;
  }

  public void setNotNullMarkers(byte[] notNullMarkers) {
    this.notNullMarkers = notNullMarkers;
  }

  public static interface KeyValueHelper extends BytesBytesMultiHashMap.KvSource {
    void setKeyValue(Writable key, Writable val) throws SerDeException;
    /** Get hash value from the key. */
    int getHashFromKey() throws SerDeException;
  }

  private static class KeyValueWriter implements KeyValueHelper {
    private final AbstractSerDe keySerDe, valSerDe;
    private final StructObjectInspector keySoi, valSoi;
    private final List<ObjectInspector> keyOis, valOis;
    private final Object[] keyObjs, valObjs;
    private final boolean hasFilterTag;

    public KeyValueWriter(
        AbstractSerDe keySerDe, AbstractSerDe valSerDe, boolean hasFilterTag) throws SerDeException {
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

    @Override
    public int getHashFromKey() throws SerDeException {
      throw new UnsupportedOperationException("Not supported for MapJoinBytesTableContainer");
    }
  }

  static class LazyBinaryKvWriter implements KeyValueHelper {
    private final LazyBinaryStruct.SingleFieldGetter filterGetter;
    private Writable key, value;
    private final AbstractSerDe keySerDe;
    private Boolean hasTag = null; // sanity check - we should not receive keys with tags

    public LazyBinaryKvWriter(AbstractSerDe keySerDe, LazyBinaryStructObjectInspector valSoi,
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

    @Override
    public int getHashFromKey() throws SerDeException {
      if (!(key instanceof BinaryComparable)) {
        throw new SerDeException("Unexpected type " + key.getClass().getCanonicalName());
      }
      sanityCheckKeyForTag();
      BinaryComparable b = (BinaryComparable)key;
      return HashCodeUtil.murmurHash(b.getBytes(), 0, b.getLength() - (hasTag ? 1 : 0));
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
      boolean[] sortableSortOrders = new boolean[fields.size()];
      Arrays.fill(sortableSortOrders, false);
      byte[] columnNullMarker = new byte[fields.size()];
      Arrays.fill(columnNullMarker, BinarySortableSerDe.ZERO);
      byte[] columnNotNullMarker = new byte[fields.size()];
      Arrays.fill(columnNotNullMarker, BinarySortableSerDe.ONE);
      BinarySortableSerDe.serializeStruct(output, data, fois, sortableSortOrders,
              columnNullMarker, columnNotNullMarker);
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

  /*
   * An implementation of KvSource that can handle key and value as BytesWritable objects.
   */
  protected static class DirectKeyValueWriter implements KeyValueHelper {

    private BytesWritable key;
    private BytesWritable val;

    @Override
    public void setKeyValue(Writable key, Writable val) throws SerDeException {
      this.key = (BytesWritable) key;
      this.val = (BytesWritable) val;
    }

    @Override
    public void writeKey(RandomAccessOutput dest) throws SerDeException {
      byte[] keyBytes = key.getBytes();
      int keyLength = key.getLength();
      dest.write(keyBytes, 0, keyLength);
    }

    @Override
    public void writeValue(RandomAccessOutput dest) throws SerDeException {
      byte[] valueBytes = val.getBytes();
      int valueLength = val.getLength();
      dest.write(valueBytes, 0 , valueLength);
    }

    @Override
    public byte updateStateByte(Byte previousValue) {
      // Not used by the direct access client -- native vector map join.
      throw new UnsupportedOperationException("Updating the state by not supported");
    }

    @Override
    public int getHashFromKey() throws SerDeException {
      byte[] keyBytes = key.getBytes();
      int keyLength = key.getLength();
      return HashCodeUtil.murmurHash(keyBytes, 0, keyLength);
    }
  }

  @Override
  public void setSerde(MapJoinObjectSerDeContext keyContext, MapJoinObjectSerDeContext valueContext)
      throws SerDeException {
    AbstractSerDe keySerde = keyContext.getSerDe(), valSerde = valueContext.getSerDe();
    if (writeHelper == null) {
      LOG.info("Initializing container with " + keySerde.getClass().getName() + " and "
          + valSerde.getClass().getName());
      if (keySerde instanceof BinarySortableSerDe && valSerde instanceof LazyBinarySerDe) {
        LazyBinaryStructObjectInspector valSoi =
            (LazyBinaryStructObjectInspector) valSerde.getObjectInspector();
        writeHelper = new LazyBinaryKvWriter(keySerde, valSoi, valueContext.hasFilterTag());
        internalValueOi = valSoi;
        sortableSortOrders = ((BinarySortableSerDe) keySerde).getSortOrders();
        nullMarkers = ((BinarySortableSerDe) keySerde).getNullMarkers();
        notNullMarkers = ((BinarySortableSerDe) keySerde).getNotNullMarkers();
      } else {
        writeHelper = new KeyValueWriter(keySerde, valSerde, valueContext.hasFilterTag());
        internalValueOi = createInternalOi(valueContext);
        sortableSortOrders = null;
        nullMarkers = null;
        notNullMarkers = null;
      }
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public MapJoinKey putRow(Writable currentKey, Writable currentValue) throws SerDeException {
    writeHelper.setKeyValue(currentKey, currentValue);
    hashMap.put(writeHelper, -1);
    return null; // there's no key to return
  }

  @Override
  public void clear() {
    // Don't clear the hash table - reuse is possible. GC will take care of it.
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

  // Direct access interfaces.

  @Override
  public void put(Writable currentKey, Writable currentValue) throws SerDeException {
    directWriteHelper.setKeyValue(currentKey, currentValue);
    hashMap.put(directWriteHelper, -1);
  }

  public static boolean hasComplexObjects(LazyBinaryStructObjectInspector lazyBinaryStructObjectInspector) {
    List<? extends StructField> fields = lazyBinaryStructObjectInspector.getAllStructFieldRefs();

    for (StructField field : fields) {
      if (field.getFieldObjectInspector().getCategory() != Category.PRIMITIVE) {
        return true;
      }
    }
    return false;
  }

  /*
   * For primitive types, use LazyBinary's object.
   * For complex types, make a standard (Java) object from LazyBinary's object.
   */
  public static List<Object> getComplexFieldsAsList(LazyBinaryStruct lazyBinaryStruct,
      ArrayList<Object> objectArrayBuffer, LazyBinaryStructObjectInspector lazyBinaryStructObjectInspector) {

    List<? extends StructField> fields = lazyBinaryStructObjectInspector.getAllStructFieldRefs();
    for (int i = 0; i < fields.size(); i++) {
      StructField field = fields.get(i);
      ObjectInspector objectInspector = field.getFieldObjectInspector();
      Category category = objectInspector.getCategory();
      Object object = lazyBinaryStruct.getField(i);
      if (category == Category.PRIMITIVE) {
        objectArrayBuffer.set(i, object);
      } else {
        objectArrayBuffer.set(i, ObjectInspectorUtils.copyToStandardObject(
            object, objectInspector, ObjectInspectorCopyOption.WRITABLE));
      }
    }
    return objectArrayBuffer;
  }

  /** Implementation of ReusableGetAdaptor that has Output for key serialization; row
   * container is also created once and reused for every row. */
  private class GetAdaptor implements ReusableGetAdaptor, ReusableGetAdaptorDirectAccess {

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
    public JoinUtil.JoinResult setFromVector(VectorHashKeyWrapper kw,
        VectorExpressionWriter[] keyOutputWriters, VectorHashKeyWrapperBatch keyWrapperBatch)
        throws HiveException {
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
      return currentValue.setFromOutput(
          MapJoinKey.serializeRow(output, currentKey, vectorKeyOIs,
                  sortableSortOrders, nullMarkers, notNullMarkers));
    }

    @Override
    public JoinUtil.JoinResult setFromRow(Object row, List<ExprNodeEvaluator> fields,
        List<ObjectInspector> ois) throws HiveException {
      if (nulls == null) {
        nulls = new boolean[fields.size()];
        currentKey = new Object[fields.size()];
      }
      for (int keyIndex = 0; keyIndex < fields.size(); ++keyIndex) {
        currentKey[keyIndex] = fields.get(keyIndex).evaluate(row);
        nulls[keyIndex] = currentKey[keyIndex] == null;
      }
      return currentValue.setFromOutput(
          MapJoinKey.serializeRow(output, currentKey, ois,
                  sortableSortOrders, nullMarkers, notNullMarkers));
    }

    @Override
    public JoinUtil.JoinResult setFromOther(ReusableGetAdaptor other) {
      assert other instanceof GetAdaptor;
      GetAdaptor other2 = (GetAdaptor)other;
      nulls = other2.nulls;
      currentKey = other2.currentKey;
      return currentValue.setFromOutput(other2.output);
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
      return !currentValue.hasRows() ? null : currentValue;
    }

    @Override
    public Object[] getCurrentKey() {
      return currentKey;
    }

    // Direct access interfaces.

    @Override
    public JoinUtil.JoinResult setDirect(byte[] bytes, int offset, int length,
        BytesBytesMultiHashMap.Result hashMapResult) {
      return currentValue.setDirect(bytes, offset, length, hashMapResult);
    }

    @Override
    public int directSpillPartitionId() {
      throw new UnsupportedOperationException("Getting the spill hash partition not supported");
    }
  }

  /** Row container that gets and deserializes the rows on demand from bytes provided. */
  private class ReusableRowContainer
    implements MapJoinRowContainer, AbstractRowContainer.RowIterator<List<Object>> {
    private byte aliasFilter;

    /** Hash table wrapper specific to the container. */
    private final BytesBytesMultiHashMap.Result hashMapResult;

    /**
     * Sometimes, when container is empty in multi-table mapjoin, we need to add a dummy row.
     * This container does not normally support adding rows; this is for the dummy row.
     */
    private List<Object> dummyRow = null;

    private final ByteArrayRef uselessIndirection; // LBStruct needs ByteArrayRef
    private final LazyBinaryStruct valueStruct;
    private final boolean needsComplexObjectFixup;
    private final ArrayList<Object> complexObjectArrayBuffer;

    public ReusableRowContainer() {
      if (internalValueOi != null) {
        valueStruct = (LazyBinaryStruct)
            LazyBinaryFactory.createLazyBinaryObject(internalValueOi);
        needsComplexObjectFixup = hasComplexObjects(internalValueOi);
        if (needsComplexObjectFixup) {
          complexObjectArrayBuffer =
              new ArrayList<Object>(
                  Collections.nCopies(internalValueOi.getAllStructFieldRefs().size(), null));
        } else {
          complexObjectArrayBuffer = null;
        }
      } else {
        valueStruct = null; // No rows?
        needsComplexObjectFixup =  false;
        complexObjectArrayBuffer = null;
      }
      uselessIndirection = new ByteArrayRef();
      hashMapResult = new BytesBytesMultiHashMap.Result();
      clearRows();
    }

    public JoinUtil.JoinResult setFromOutput(Output output) {

      aliasFilter = hashMap.getValueResult(
              output.getData(), 0, output.getLength(), hashMapResult);
      dummyRow = null;
      if (hashMapResult.hasRows()) {
        return JoinUtil.JoinResult.MATCH;
      } else {
        aliasFilter = (byte) 0xff;
        return JoinUtil.JoinResult.NOMATCH;
      }

   }

    @Override
    public boolean hasRows() {
      return hashMapResult.hasRows() || (dummyRow != null);
    }

    @Override
    public boolean isSingleRow() {
      if (!hashMapResult.hasRows()) {
        return (dummyRow != null);
      }
      return hashMapResult.isSingleRow();
    }

    // Implementation of row container
    @Override
    public AbstractRowContainer.RowIterator<List<Object>> rowIter() throws HiveException {
      return this;
    }

    @Override
    public int rowCount() throws HiveException {
      // For performance reasons we do not want to chase the values to the end to determine
      // the count.  Use hasRows and isSingleRow instead.
      throw new UnsupportedOperationException("Getting the row count not supported");
    }

    @Override
    public void clearRows() {
      // Doesn't clear underlying hashtable
      hashMapResult.forget();
      dummyRow = null;
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

      // A little strange that we forget the dummy row on read.
      if (dummyRow != null) {
        List<Object> result = dummyRow;
        dummyRow = null;
        return result;
      }

      WriteBuffers.ByteSegmentRef byteSegmentRef = hashMapResult.first();
      if (byteSegmentRef == null) {
        return null;
      } else {
        return unpack(byteSegmentRef);
      }

    }

    @Override
    public List<Object> next() throws HiveException {

      WriteBuffers.ByteSegmentRef byteSegmentRef = hashMapResult.next();
      if (byteSegmentRef == null) {
        return null;
      } else {
        return unpack(byteSegmentRef);
      }

    }

    private List<Object> unpack(WriteBuffers.ByteSegmentRef ref) throws HiveException {
      if (ref.getLength() == 0) {
        return EMPTY_LIST; // shortcut, 0 length means no fields
      }
      uselessIndirection.setData(ref.getBytes());
      valueStruct.init(uselessIndirection, (int)ref.getOffset(), ref.getLength());
      List<Object> result;
      if (!needsComplexObjectFixup) {
        // Good performance for common case where small table has no complex objects.
        result = valueStruct.getFieldsAsList();
      } else {
        // Convert the complex LazyBinary objects to standard (Java) objects so downstream
        // operators like FileSinkOperator can serialize complex objects in the form they expect
        // (i.e. Java objects).
        result = getComplexFieldsAsList(
            valueStruct, complexObjectArrayBuffer, internalValueOi);
      }
      return result;
    }

    @Override
    public void addRow(List<Object> t) {
      if (dummyRow != null || hashMapResult.hasRows()) {
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

    // Direct access.

    public JoinUtil.JoinResult setDirect(byte[] bytes, int offset, int length,
        BytesBytesMultiHashMap.Result hashMapResult) {
      aliasFilter = hashMap.getValueResult(bytes, offset, length, hashMapResult);
      dummyRow = null;
      if (hashMapResult.hasRows()) {
        return JoinUtil.JoinResult.MATCH;
      } else {
        aliasFilter = (byte) 0xff;
        return JoinUtil.JoinResult.NOMATCH;
      }
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

  @Override
  public boolean hasSpill() {
    return false;
  }

  @Override
  public int size() {
    return hashMap.size();
  }
}
