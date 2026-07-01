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

package org.apache.hadoop.hive.ql.anon.extract;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.anon.ConstCode;
import org.apache.hadoop.hive.ql.anon.convert.AvroBodyConverter;
import org.apache.hadoop.hive.ql.anon.convert.BodyConverter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class AvroMessageExtractor implements Extractor {

  private Configuration conf;
  private final BodyConverter bodyConverter;

  public AvroMessageExtractor(final ConstCode formatCode) {
    if (formatCode == ConstCode.a) {
      bodyConverter = new AvroBodyConverter();
    } else {
      throw new IllegalArgumentException("bad code");
    }
  }

  @Override
  public void extractIdentifyFieldValues(final Writable fldName, final WritableComparable msgId, final Writable body, final Set<WritableComparable> set) {
    String fieldName = fldName.toString();
    SpecificRecordBase msg = (SpecificRecordBase) bodyConverter.convertBody(msgId, body);
    AvroMessageExtractor.extract2(fieldName, msg, set);
  }

  @Override
  public void extract(Writable fldName, Object msg, Set<WritableComparable> ids) {
    String fieldName = fldName.toString();
    AvroMessageExtractor.extract2(fieldName, (SpecificRecordBase) msg, ids);
  }

  @Override
  public boolean containsIdentityField(final String fldName, final WritableComparable msgId, final Writable body) {
    SpecificRecordBase msg = (SpecificRecordBase) bodyConverter.convertBody(msgId, body);
    return contains2(msg.getClass(), fldName);
  }

  public static void extract2(final String idFieldName, final SpecificRecordBase msg, final Set<WritableComparable> set) {
    final String normalizedFieldName = idFieldName;
    Class<?> clazz = msg.getClass();
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      if (field.getName().equals(normalizedFieldName)) {
        getFieldValue2(field, msg, set);
      } else {
        Class<?> fieldType = field.getType();
        if (SpecificRecordBase.class.isAssignableFrom(fieldType)) {
          try {
            field.setAccessible(true);
            final SpecificRecordBase sub = (SpecificRecordBase) field.get(msg);
            if (sub != null) {
              extract2(idFieldName, sub, set);
            }
          } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        } else if (fieldType.getSimpleName().equals("List")) {
          ParameterizedType pt = (ParameterizedType) field.getGenericType();
          Class<?> ptc = (Class<?>) pt.getActualTypeArguments()[0];
          if (SpecificRecordBase.class.isAssignableFrom(ptc)) {
            try {
              field.setAccessible(true);
              final List lst = (List) field.get(msg);
              if (lst != null) {
                for (Object o : lst) {
                  if (o != null) {
                    extract2(idFieldName, (SpecificRecordBase) o, set);
                  }
                }
              }
            } catch (IllegalAccessException e) {
              throw new RuntimeException(e);
            }
          }
        }
      }
    }
  }

  private static void getFieldValue2(Field f, SpecificRecordBase msg, Set<WritableComparable> set) {
    if (f == null) {
      return;
    }
    f.setAccessible(true);
    try {
      Class<?> type = f.getType();
      String typeName = type.getSimpleName();
      switch (typeName) {
        case "int": {
          set.add(new IntWritable(f.getInt(msg)));
          break;
        }
        case "long": {
          set.add(new LongWritable(f.getLong(msg)));
          break;
        }
        default: {
          throw new RuntimeException("unsupported type " + typeName);
        }
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean contains2(final Class<? extends SpecificRecordBase> clazz, final String fieldName) {
    final String normalizedFieldName = fieldName;
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      int modifiers = field.getModifiers();
      if (Modifier.isStatic(modifiers) || !Modifier.isPrivate(modifiers)) {
        continue;
      }
      if (normalizedFieldName.equals(field.getName())) {
        return true;
      }
      Class<?> type = field.getType();
      if (SpecificRecordBase.class.isAssignableFrom(type)) {
        if (contains2((Class<? extends SpecificRecordBase>) type, fieldName)) {
          return true;
        }
      }
      if (List.class.isAssignableFrom(type)) {
        ParameterizedType gt = (ParameterizedType) field.getGenericType();
        Type at = gt.getActualTypeArguments()[0];
        Class<?> cat = (Class<?>) at;
        if (SpecificRecordBase.class.isAssignableFrom(cat)) {
          if (contains2((Class<? extends SpecificRecordBase>) cat, fieldName)) {
            return true;
          }
        }
      }
    }
    return false;
  }

}
