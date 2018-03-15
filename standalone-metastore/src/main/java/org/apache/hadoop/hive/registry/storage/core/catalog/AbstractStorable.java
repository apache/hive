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

package org.apache.hadoop.hive.registry.storage.core.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.hadoop.hive.registry.common.Schema;
import org.apache.hadoop.hive.registry.common.exception.ParserException;
import org.apache.hadoop.hive.registry.common.util.ReflectionHelper;
import org.apache.hadoop.hive.registry.storage.core.Storable;
import org.apache.hadoop.hive.registry.storage.core.annotation.SchemaIgnore;
import org.apache.hadoop.hive.registry.storage.core.exception.StorageException;
import org.apache.hadoop.hive.registry.storage.core.StorableKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Default implementations go here
 */
public abstract class AbstractStorable implements Storable {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStorable.class);

    @JsonIgnore
    public StorableKey getStorableKey() {
        return new StorableKey(getNameSpace(), getPrimaryKey());
    }

    /**
     * Default implementation that will read all the instance variable names using API and
     * get the value by calling getter method (POJO) convention on it.
     *
     * Sometimes for JDBC to work we need an extra layer of transformation , for example see the implementation
     * in {@code DataSource} which defines a field of type @{code Type} which is enum and not a primitive type as expected
     * by the JDBC layer, you can call this method and override the fields that needs transformation.
     *
     * @return the map
     */
    public Map<String, Object> toMap() {
        Set<String> instanceVariableNames = ReflectionHelper.getFieldNamesToTypes(this.getClass()).keySet();
        Map<String, Object> fieldToVal = new HashMap<>();
        for(String fieldName : instanceVariableNames) {
            try {
                Object val = ReflectionHelper.invokeGetter(fieldName, this);
                fieldToVal.put(fieldName, val);
                if(LOG.isTraceEnabled()) {
                    LOG.trace("toMap: Adding fieldName {} = {} ", fieldName, val);
                }
            } catch (NoSuchMethodException|InvocationTargetException|IllegalAccessException e) {
                throw new StorageException(e);
            }
        }

        return fieldToVal;
    }

    /**
     * Default implementation that will read all the instance variable names and invoke setter.
     *
     * Same as the toMap() method you should override this method when a field's defined type is not a primitive.
     *
     * @param map the map
     * @return the storable
     */
    public Storable fromMap(Map<String, Object> map) {
        for(Map.Entry<String, Object> entry: map.entrySet()) {
            try {
                if(entry.getValue() != null) {
                    ReflectionHelper.invokeSetter(entry.getKey(), this, entry.getValue());
                }
            } catch (NoSuchMethodException|InvocationTargetException|IllegalAccessException e) {
                throw new StorageException(e);
            }
        }
        return this;
    }

    /**
     * Default implementation that will generate schema by reading all the field names in the class and use its
     * define type to convert to the Schema type.
     *
     * @return the schema
     */
    @Override
    @JsonIgnore
    public Schema getSchema() {
        Map<String, Class> fieldNamesToTypes = ReflectionHelper.getFieldNamesToTypes(this.getClass());
        List<Schema.Field> fields = new ArrayList<>();

        for(Map.Entry<String, Class> entry : fieldNamesToTypes.entrySet()) {
            try {
                getField(entry.getKey(), entry.getValue()).ifPresent(field -> {
                    fields.add(field);
                    LOG.trace("getSchema: Adding {}", field);
                });
            } catch (NoSuchFieldException|NoSuchMethodException|InvocationTargetException|IllegalAccessException|ParserException e) {
                throw new StorageException(e);
            }
        }

        return Schema.of(fields);
    }

    private Optional<Schema.Field> getField(String name, Class<?> clazz) throws NoSuchFieldException,
            NoSuchMethodException, IllegalAccessException, InvocationTargetException, ParserException {
        Field field = this.getClass().getDeclaredField(name);
        if (field.getAnnotation(SchemaIgnore.class) != null) {
            LOG.debug("Ignoring field {}", field);
            return Optional.empty();
        }
        Object val = ReflectionHelper.invokeGetter(name, this);
        Schema.Type type;
        if (val != null) {
            type = Schema.fromJavaType(val);
        } else {
            type = Schema.fromJavaType(clazz);
        }
        return Optional.of(new Schema.Field(name, type));
    }

    @Override
    public Long getId() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void setId(Long id) {
        throw new UnsupportedOperationException("Not implemented");
    }

}
