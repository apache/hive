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
package org.apache.hadoop.hive.registry.common.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ReflectionHelper {
    private static final Logger LOG = LoggerFactory.getLogger(ReflectionHelper.class);

    public static <T> T newInstance(String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return (T) Class.forName(className).newInstance();
    }

    public static <T> T invokeGetter(String propertyName, Object object) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String methodName = "get" + StringUtils.capitalize(propertyName);
        Method method = object.getClass().getMethod(methodName);
        return (T) method.invoke(object);
    }

    public static <T> T invokeSetter(String propertyName, Object object, Object valueToSet) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String methodName = "set" + StringUtils.capitalize(propertyName);
        Method method = null;
        try {
            method = object.getClass().getMethod(methodName, valueToSet.getClass());
        } catch (NoSuchMethodException ex) {
            // try setters that accept super types
            Method[] methods = object.getClass().getMethods();
            for (int i = 0; i < methods.length; i++) {
                if (methods[i].getName().equals(methodName) && methods[i].getParameterCount() == 1) {
                    if (methods[i].getParameters()[0].getType().isAssignableFrom(valueToSet.getClass())) {
                        method = methods[i];
                        break;
                    }
                }
            }
            if (method == null) {
                throw ex;
            }
        }
        return (T) method.invoke(object, valueToSet);
    }

    /**
     * Given a class, this method returns a map of names of all the instance (non static) fields to type.
     * if the class has any super class it also includes those fields.
     * @param clazz , not null
     * @return
     */
    public static Map<String, Class> getFieldNamesToTypes(Class clazz) {
        Field[] declaredFields = clazz.getDeclaredFields();
        Map<String, Class> instanceVariableNamesToTypes = new HashMap<>();
        for(Field field : declaredFields) {
            if(!Modifier.isStatic(field.getModifiers())) {
                LOG.trace("clazz {} has field {} with type {}", clazz.getName(), field.getName(), field.getType().getName());
                instanceVariableNamesToTypes.put(field.getName(), field.getType());
            } else {
                LOG.trace("clazz {} has field {} with type {}, which is static so ignoring", clazz.getName(), field.getName(), field.getType().getName());
            }
        }

        if(!clazz.getSuperclass().equals(Object.class)) {
            instanceVariableNamesToTypes.putAll(getFieldNamesToTypes(clazz.getSuperclass()));
        }
        return instanceVariableNamesToTypes;
    }

    public static Collection<Class<?>> getAnnotatedClasses(String basePackage, Class<? extends Annotation> annotation) {
        Collection<Class<?>> classes = new ArrayList<>();
        ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false);
        provider.addIncludeFilter(new AnnotationTypeFilter(annotation));
        for (BeanDefinition beanDef : provider.findCandidateComponents(basePackage)) {
            try {
                classes.add(Class.forName(beanDef.getBeanClassName()));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        return classes;
    }

}
