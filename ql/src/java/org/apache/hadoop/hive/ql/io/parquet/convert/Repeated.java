package org.apache.hadoop.hive.ql.io.parquet.convert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import parquet.column.Dictionary;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.PrimitiveType;

/**
 * Converters for repeated fields need to know when the parent field starts and
 * ends to correctly build lists from the repeated values.
 */
public interface Repeated extends ConverterParent {

  public void parentStart();

  public void parentEnd();

  abstract class RepeatedConverterParent extends PrimitiveConverter implements Repeated {
    private Map<String, String> metadata;

    public void setMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
    }

    public Map<String, String> getMetadata() {
      return metadata;
    }
  }

  /**
   * Stands in for a PrimitiveConverter and accumulates multiple values as an
   * ArrayWritable.
   */
  class RepeatedPrimitiveConverter extends RepeatedConverterParent {
    private final PrimitiveType primitiveType;
    private final PrimitiveConverter wrapped;
    private final ConverterParent parent;
    private final int index;
    private final List<Writable> list = new ArrayList<Writable>();

    public RepeatedPrimitiveConverter(PrimitiveType primitiveType, ConverterParent parent, int index) {
      setMetadata(parent.getMetadata());
      this.primitiveType = primitiveType;
      this.parent = parent;
      this.index = index;
      this.wrapped = HiveGroupConverter.getConverterFromDescription(primitiveType, 0, this);
    }

    @Override
    public boolean hasDictionarySupport() {
      return wrapped.hasDictionarySupport();
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      wrapped.setDictionary(dictionary);
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      wrapped.addValueFromDictionary(dictionaryId);
    }

    @Override
    public void addBinary(Binary value) {
      wrapped.addBinary(value);
    }

    @Override
    public void addBoolean(boolean value) {
      wrapped.addBoolean(value);
    }

    @Override
    public void addDouble(double value) {
      wrapped.addDouble(value);
    }

    @Override
    public void addFloat(float value) {
      wrapped.addFloat(value);
    }

    @Override
    public void addInt(int value) {
      wrapped.addInt(value);
    }

    @Override
    public void addLong(long value) {
      wrapped.addLong(value);
    }

    @Override
    public void parentStart() {
      list.clear();
    }

    @Override
    public void parentEnd() {
      parent.set(index, HiveGroupConverter.wrapList(new ArrayWritable(
          Writable.class, list.toArray(new Writable[list.size()]))));
    }

    @Override
    public void set(int index, Writable value) {
      list.add(value);
    }
  }

  /**
   * Stands in for a HiveGroupConverter and accumulates multiple values as an
   * ArrayWritable.
   */
  class RepeatedGroupConverter extends HiveGroupConverter
      implements Repeated {
    private final GroupType groupType;
    private final HiveGroupConverter wrapped;
    private final ConverterParent parent;
    private final int index;
    private final List<Writable> list = new ArrayList<Writable>();
    private final Map<String, String> metadata = new HashMap<String, String>();


    public RepeatedGroupConverter(GroupType groupType, ConverterParent parent, int index) {
      setMetadata(parent.getMetadata());
      this.groupType = groupType;
      this.parent = parent;
      this.index = index;
      this.wrapped = HiveGroupConverter.getConverterFromDescription(groupType, 0, this);
    }

    @Override
    public void set(int fieldIndex, Writable value) {
      list.add(value);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      // delegate to the group's converters
      return wrapped.getConverter(fieldIndex);
    }

    @Override
    public void start() {
      wrapped.start();
    }

    @Override
    public void end() {
      wrapped.end();
    }

    @Override
    public void parentStart() {
      list.clear();
    }

    @Override
    public void parentEnd() {
      parent.set(index, wrapList(new ArrayWritable(
          Writable.class, list.toArray(new Writable[list.size()]))));
    }
  }
}
