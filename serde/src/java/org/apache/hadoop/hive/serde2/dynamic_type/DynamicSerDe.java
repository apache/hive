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

package org.apache.hadoop.hive.serde2.dynamic_type;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.thrift.ConfigurableTProtocol;
import org.apache.hadoop.hive.serde2.thrift.TReflectionUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

/**
 * DynamicSerDe.
 *
 */
public class DynamicSerDe extends AbstractSerDe {

  public static final Log LOG = LogFactory.getLog(DynamicSerDe.class.getName());

  private String type_name;
  private DynamicSerDeStructBase bt;

  public static final String META_TABLE_NAME = "name";

  private transient thrift_grammar parse_tree;
  protected transient ByteStream.Input bis_;
  protected transient ByteStream.Output bos_;

  /**
   * protocols are protected in case any of their properties need to be queried
   * from another class in this package. For TCTLSeparatedProtocol for example,
   * may want to query the separators.
   */
  protected transient TProtocol oprot_;
  protected transient TProtocol iprot_;

  TIOStreamTransport tios;

  @Override
  public void initialize(Configuration job, Properties tbl) throws SerDeException {
    try {

      String ddl = tbl.getProperty(serdeConstants.SERIALIZATION_DDL);
      // type_name used to be tbl.getProperty(META_TABLE_NAME).
      // However, now the value is DBName.TableName. To make it backward compatible,
      // we take the TableName part as type_name.
      //
      String tableName = tbl.getProperty(META_TABLE_NAME);
      int index = tableName.indexOf('.');
      if (index != -1) {
        type_name = tableName.substring(index + 1, tableName.length());
      } else {
        type_name = tableName;
      }
      String protoName = tbl.getProperty(serdeConstants.SERIALIZATION_FORMAT);

      if (protoName == null) {
        protoName = "org.apache.thrift.protocol.TBinaryProtocol";
      }
      // For backward compatibility
      protoName = protoName.replace("com.facebook.thrift.protocol",
          "org.apache.thrift.protocol");
      TProtocolFactory protFactory = TReflectionUtils
          .getProtocolFactoryByName(protoName);
      bos_ = new ByteStream.Output();
      bis_ = new ByteStream.Input();
      tios = new TIOStreamTransport(bis_, bos_);

      oprot_ = protFactory.getProtocol(tios);
      iprot_ = protFactory.getProtocol(tios);

      /**
       * initialize the protocols
       */

      if (oprot_ instanceof org.apache.hadoop.hive.serde2.thrift.ConfigurableTProtocol) {
        ((ConfigurableTProtocol) oprot_).initialize(job, tbl);
      }

      if (iprot_ instanceof org.apache.hadoop.hive.serde2.thrift.ConfigurableTProtocol) {
        ((ConfigurableTProtocol) iprot_).initialize(job, tbl);
      }

      // in theory the include path should come from the configuration
      List<String> include_path = new ArrayList<String>();
      include_path.add(".");
      LOG.debug("ddl=" + ddl);
      parse_tree = new thrift_grammar(new ByteArrayInputStream(ddl.getBytes()),
          include_path, false);
      parse_tree.Start();

      bt = (DynamicSerDeStructBase) parse_tree.types.get(type_name);

      if (bt == null) {
        bt = (DynamicSerDeStructBase) parse_tree.tables.get(type_name);
      }

      if (bt == null) {
        throw new SerDeException("Could not lookup table type " + type_name
            + " in this ddl: " + ddl);
      }

      bt.initialize();
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      throw new SerDeException(e);
    }
  }

  Object deserializeReuse = null;

  @Override
  public Object deserialize(Writable field) throws SerDeException {
    try {
      if (field instanceof Text) {
        Text b = (Text) field;
        bis_.reset(b.getBytes(), b.getLength());
      } else {
        BytesWritable b = (BytesWritable) field;
        bis_.reset(b.getBytes(), b.getLength());
      }
      deserializeReuse = bt.deserialize(deserializeReuse, iprot_);
      return deserializeReuse;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SerDeException(e);
    }
  }

  public static ObjectInspector dynamicSerDeStructBaseToObjectInspector(
      DynamicSerDeTypeBase bt) throws SerDeException {
    if (bt.isList()) {
      return ObjectInspectorFactory
          .getStandardListObjectInspector(dynamicSerDeStructBaseToObjectInspector(((DynamicSerDeTypeList) bt)
              .getElementType()));
    } else if (bt.isMap()) {
      DynamicSerDeTypeMap btMap = (DynamicSerDeTypeMap) bt;
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          dynamicSerDeStructBaseToObjectInspector(btMap.getKeyType()),
          dynamicSerDeStructBaseToObjectInspector(btMap.getValueType()));
    } else if (bt.isPrimitive()) {
      PrimitiveTypeEntry pte = PrimitiveObjectInspectorUtils
          .getTypeEntryFromPrimitiveJavaClass(bt.getRealType());
      return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(pte.primitiveCategory);
    } else {
      // Must be a struct
      DynamicSerDeStructBase btStruct = (DynamicSerDeStructBase) bt;
      DynamicSerDeFieldList fieldList = btStruct.getFieldList();
      DynamicSerDeField[] fields = fieldList.getChildren();
      ArrayList<String> fieldNames = new ArrayList<String>(fields.length);
      ArrayList<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
          fields.length);
      for (DynamicSerDeField field : fields) {
        fieldNames.add(field.name);
        fieldObjectInspectors.add(dynamicSerDeStructBaseToObjectInspector(field
            .getFieldType().getMyType()));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(
          fieldNames, fieldObjectInspectors);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return dynamicSerDeStructBaseToObjectInspector(bt);
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  BytesWritable ret = new BytesWritable();

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    try {
      bos_.reset();
      bt.serialize(obj, objInspector, oprot_);
      oprot_.getTransport().flush();
    } catch (Exception e) {
      e.printStackTrace();
      throw new SerDeException(e);
    }
    ret.set(bos_.getData(), 0, bos_.getCount());
    return ret;
  }


  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }
}
