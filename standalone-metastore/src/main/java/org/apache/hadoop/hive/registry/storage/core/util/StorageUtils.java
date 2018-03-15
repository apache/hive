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
package org.apache.hadoop.hive.registry.storage.core.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.registry.common.QueryParam;
import org.apache.hadoop.hive.registry.common.exception.DuplicateEntityException;
import org.apache.hadoop.hive.registry.common.util.ReflectionHelper;
import org.apache.hadoop.hive.registry.storage.core.Storable;
import org.apache.hadoop.hive.registry.storage.core.annotation.StorableEntity;
import org.apache.hadoop.hive.registry.storage.core.annotation.SearchableField;
import org.apache.hadoop.hive.registry.storage.core.annotation.VersionField;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.hadoop.hive.registry.common.util.ReflectionHelper.getAnnotatedClasses;

/**
 * Utility methods for the storage package.
 */
public final class StorageUtils {


    private StorageUtils() {
    }

    public static <T extends Storable> T jsonToStorable(String json, Class<T> clazz) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(json, clazz);
    }

    public static String storableToJson(Storable storable) throws IOException {
        return storable != null ? new ObjectMapper().writeValueAsString(storable) : null;
    }

    public static void ensureUnique(Storable storable,
                                    Function<List<QueryParam>, Collection<? extends Storable>> listFn,
                                    List<QueryParam> queryParams) {
        Collection<? extends Storable> storables = listFn.apply(queryParams);
        Optional<Long> entities = storables.stream()
                .map(Storable::getId)
                .filter(x -> !x.equals(storable.getId()))
                .findAny();
        if (entities.isPresent()) {
            throw new DuplicateEntityException("Entity with '" + queryParams + "' already exists");
        }
    }

    @SuppressWarnings("unchecked")
    public static Collection<Class<? extends Storable>> getStorableEntities() {
        Set<Class<? extends Storable>> entities = new HashSet<>();
        ReflectionHelper.getAnnotatedClasses("com.hortonworks", StorableEntity.class).forEach(clazz -> {
            if (Storable.class.isAssignableFrom(clazz)) {
                entities.add((Class<? extends Storable>) clazz);
            }
        });
        return entities;
    }

    public static List<Pair<Field, String>> getSearchableFieldValues(Storable storable)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        List<Pair<Field, String>> res = new ArrayList<>();
        getAnnotatedFieldValues(storable, SearchableField.class).forEach(kv -> {
            res.add(Pair.of(kv.getKey(), kv.getValue() instanceof String ? (String) kv.getValue() : kv.getValue().toString()));
        });
        return res;
    }

    public static Optional<Pair<Field, Long>> getVersionFieldValue(Storable storable)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        for (Pair<Field, Object> kv : getAnnotatedFieldValues(storable, VersionField.class)) {
            if (kv.getValue() instanceof Long) {
                return Optional.of(Pair.of(kv.getKey(), (Long) kv.getValue()));
            }
        }
        return Optional.empty();
    }

    public static List<Pair<Field, Object>> getAnnotatedFieldValues(Storable storable, Class<? extends Annotation> clazz)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        List<Pair<Field, Object>> res = new ArrayList<>();
        for (Field field : storable.getClass().getDeclaredFields()) {
            if (field.getAnnotation(clazz) != null) {
                Object val = ReflectionHelper.invokeGetter(field.getName(), storable);
                if (val != null) {
                    res.add(Pair.of(field, val));
                }
            }
        }
        return res;
    }
}
