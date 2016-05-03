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

package org.apache.hadoop.hive.ql.udf.generic;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;


public abstract class BaseMaskUDF extends GenericUDF {
  private static final Log LOG = LogFactory.getLog(BaseMaskUDF.class);

  final protected AbstractTransformer  transformer;
  final protected String               displayName;
  protected AbstractTransformerAdapter transformerAdapter = null;

  protected BaseMaskUDF(AbstractTransformer transformer, String displayName) {
    this.transformer = transformer;
    this.displayName = displayName;
  }

  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    LOG.debug("==> BaseMaskUDF.initialize()");

    checkArgPrimitive(arguments, 0); // first argument is the column to be transformed

    PrimitiveObjectInspector columnType = ((PrimitiveObjectInspector) arguments[0]);

    transformer.init(arguments, 1);

    transformerAdapter = AbstractTransformerAdapter.getTransformerAdapter(columnType, transformer);

    ObjectInspector ret = PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(columnType.getPrimitiveCategory());

    LOG.debug("<== BaseMaskUDF.initialize()");

    return ret;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object ret = transformerAdapter.getTransformedWritable(arguments[0]);

    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(displayName, children);
  }
}


/**
 * Interface to be implemented by transformers which transform a given value according to its specification.
 */
abstract class AbstractTransformer {
  /**
   * Initialzie the transformer object
   * @param arguments arguments given to GenericUDF.initialzie()
   * @param startIdx index into array, from which the transformer should read values
   */
  abstract void init(ObjectInspector[] arguments, int startIdx);

  /**
   * Transform a String value
   * @param value value to transform
   * @return transformed value
   */
  abstract String transform(String value);

  /**
   * Transform a Byte value
   * @param value value to transform
   * @return transformed value
   */
  abstract Byte transform(Byte value);

  /**
   * Transform a Short value
   * @param value value to transform
   * @return transformed value
   */
  abstract Short transform(Short value);

  /**
   * Transform a Integer value
   * @param value value to transform
   * @return transformed value
   */
  abstract Integer transform(Integer value);

  /**
   * Transform a Long value
   * @param value value to transform
   * @return transformed value
   */
  abstract Long transform(Long value);

  /**
   * Transform a Date value
   * @param value value to transform
   * @return transformed value
   */
  abstract Date transform(Date value);
}

/**
 * Interface to be implemented by datatype specific adapters that handle necessary conversion of the transformed value
 * into appropriate Writable object, which GenericUDF.evaluate() is expected to return.
 */
abstract class AbstractTransformerAdapter {
  final AbstractTransformer transformer;

  AbstractTransformerAdapter(AbstractTransformer transformer) {
    this.transformer = transformer;
  }

  abstract Object getTransformedWritable(DeferredObject value) throws HiveException;

  static AbstractTransformerAdapter getTransformerAdapter(PrimitiveObjectInspector columnType, AbstractTransformer transformer) {
    final AbstractTransformerAdapter ret;

    switch(columnType.getPrimitiveCategory()) {
      case STRING:
        ret = new StringTransformerAdapter((StringObjectInspector)columnType, transformer);
        break;

      case CHAR:
        ret = new HiveCharTransformerAdapter((HiveCharObjectInspector)columnType, transformer);
        break;

      case VARCHAR:
        ret = new HiveVarcharTransformerAdapter((HiveVarcharObjectInspector)columnType, transformer);
        break;

      case BYTE:
        ret = new ByteTransformerAdapter((ByteObjectInspector)columnType, transformer);
        break;

      case SHORT:
        ret = new ShortTransformerAdapter((ShortObjectInspector)columnType, transformer);
        break;

      case INT:
        ret = new IntegerTransformerAdapter((IntObjectInspector)columnType, transformer);
        break;

      case LONG:
        ret = new LongTransformerAdapter((LongObjectInspector)columnType, transformer);
        break;

      case DATE:
        ret = new DateTransformerAdapter((DateObjectInspector)columnType, transformer);
        break;

      default:
        ret = new UnsupportedDatatypeTransformAdapter(columnType, transformer);
        break;
    }

    return ret;
  }
}

class ByteTransformerAdapter extends AbstractTransformerAdapter {
  final ByteObjectInspector columnType;
  final ByteWritable        writable;

  public ByteTransformerAdapter(ByteObjectInspector columnType, AbstractTransformer transformer) {
    this(columnType, transformer, new ByteWritable());
  }

  public ByteTransformerAdapter(ByteObjectInspector columnType, AbstractTransformer transformer, ByteWritable writable) {
    super(transformer);

    this.columnType = columnType;
    this.writable   = writable;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    Byte value = (Byte)columnType.getPrimitiveJavaObject(object.get());

    if(value != null) {
      Byte transformedValue = transformer.transform(value);

      if(transformedValue != null) {
        writable.set(transformedValue);

        return writable;
      }
    }

    return null;
  }
}

class DateTransformerAdapter extends AbstractTransformerAdapter {
  final DateObjectInspector columnType;
  final DateWritable        writable;

  public DateTransformerAdapter(DateObjectInspector columnType, AbstractTransformer transformer) {
    this(columnType, transformer, new DateWritable());
  }

  public DateTransformerAdapter(DateObjectInspector columnType, AbstractTransformer transformer, DateWritable writable) {
    super(transformer);

    this.columnType = columnType;
    this.writable   = writable;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    Date value = columnType.getPrimitiveJavaObject(object.get());

    if(value != null) {
      Date transformedValue = transformer.transform(value);

      if(transformedValue != null) {
        writable.set(transformedValue);

        return writable;
      }
    }

    return null;
  }
}

class HiveCharTransformerAdapter extends AbstractTransformerAdapter {
  final HiveCharObjectInspector columnType;
  final HiveCharWritable        writable;

  public HiveCharTransformerAdapter(HiveCharObjectInspector columnType, AbstractTransformer transformer) {
    this(columnType, transformer, new HiveCharWritable());
  }

  public HiveCharTransformerAdapter(HiveCharObjectInspector columnType, AbstractTransformer transformer, HiveCharWritable writable) {
    super(transformer);

    this.columnType = columnType;
    this.writable   = writable;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    HiveChar value = columnType.getPrimitiveJavaObject(object.get());

    if(value != null) {
      String transformedValue = transformer.transform(value.getValue());

      if(transformedValue != null) {
        writable.set(transformedValue);

        return writable;
      }
    }

    return null;
  }
}

class HiveVarcharTransformerAdapter extends AbstractTransformerAdapter {
  final HiveVarcharObjectInspector columnType;
  final HiveVarcharWritable        writable;

  public HiveVarcharTransformerAdapter(HiveVarcharObjectInspector columnType, AbstractTransformer transformer) {
    this(columnType, transformer, new HiveVarcharWritable());
  }

  public HiveVarcharTransformerAdapter(HiveVarcharObjectInspector columnType, AbstractTransformer transformer, HiveVarcharWritable writable) {
    super(transformer);

    this.columnType = columnType;
    this.writable   = writable;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    HiveVarchar value = columnType.getPrimitiveJavaObject(object.get());

    if(value != null) {
      String transformedValue = transformer.transform(value.getValue());

      if(transformedValue != null) {
        writable.set(transformedValue);

        return writable;
      }
    }

    return null;
  }
}

class IntegerTransformerAdapter extends AbstractTransformerAdapter {
  final IntObjectInspector columnType;
  final IntWritable        writable;

  public IntegerTransformerAdapter(IntObjectInspector columnType, AbstractTransformer transformer) {
    this(columnType, transformer, new IntWritable());
  }

  public IntegerTransformerAdapter(IntObjectInspector columnType, AbstractTransformer transformer, IntWritable writable) {
    super(transformer);

    this.columnType = columnType;
    this.writable   = writable;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    Integer value = (Integer)columnType.getPrimitiveJavaObject(object.get());

    if(value != null) {
      Integer transformedValue = transformer.transform(value);

      if(transformedValue != null) {
        writable.set(transformedValue);

        return writable;
      }
    }

    return null;
  }
}

class LongTransformerAdapter extends AbstractTransformerAdapter {
  final LongObjectInspector columnType;
  final LongWritable        writable;

  public LongTransformerAdapter(LongObjectInspector columnType, AbstractTransformer transformer) {
    this(columnType, transformer, new LongWritable());
  }

  public LongTransformerAdapter(LongObjectInspector columnType, AbstractTransformer transformer, LongWritable writable) {
    super(transformer);

    this.columnType = columnType;
    this.writable   = writable;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    Long value = (Long)columnType.getPrimitiveJavaObject(object.get());

    if(value != null) {
      Long transformedValue = transformer.transform(value);

      if(transformedValue != null) {
        writable.set(transformedValue);

        return writable;
      }
    }

    return null;
  }
}

class ShortTransformerAdapter extends AbstractTransformerAdapter {
  final ShortObjectInspector columnType;
  final ShortWritable        writable;

  public ShortTransformerAdapter(ShortObjectInspector columnType, AbstractTransformer transformer) {
    this(columnType, transformer, new ShortWritable());
  }

  public ShortTransformerAdapter(ShortObjectInspector columnType, AbstractTransformer transformer, ShortWritable writable) {
    super(transformer);

    this.columnType = columnType;
    this.writable   = writable;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    Short value = (Short)columnType.getPrimitiveJavaObject(object.get());

    if(value != null) {
      Short transformedValue = transformer.transform(value);

      if(transformedValue != null) {
        writable.set(transformedValue);

        return writable;
      }
    }

    return null;
  }
}

class StringTransformerAdapter extends AbstractTransformerAdapter {
  final StringObjectInspector columnType;
  final Text                  writable;

  public StringTransformerAdapter(StringObjectInspector columnType, AbstractTransformer transformer) {
    this(columnType, transformer, new Text());
  }

  public StringTransformerAdapter(StringObjectInspector columnType, AbstractTransformer transformer, Text writable) {
    super(transformer);

    this.columnType = columnType;
    this.writable   = writable;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    String value = columnType.getPrimitiveJavaObject(object.get());

    if(value != null) {
      String transformedValue = transformer.transform(value);

      if(transformedValue != null) {
        writable.set(transformedValue);

        return writable;
      }
    }

    return null;
  }
}

class UnsupportedDatatypeTransformAdapter extends AbstractTransformerAdapter {
  final PrimitiveObjectInspector columnType;

  public UnsupportedDatatypeTransformAdapter(PrimitiveObjectInspector columnType, AbstractTransformer transformer) {
    super(transformer);

    this.columnType = columnType;
  }

  @Override
  public Object getTransformedWritable(DeferredObject object) throws HiveException {
    return null;
  }
}
