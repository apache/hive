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

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

public class ProxyUtil<O> {
    private final Class<O> interfaceClazz;
    private final ClassLoader parentClassLoader;
    private final ConcurrentHashMap<String, ClassLoader> cachedClassLoaders;

    public ProxyUtil(Class<O> interfaceClazz) {
        this(interfaceClazz, ProxyUtil.class.getClassLoader());
    }

    public ProxyUtil(Class<O> interfaceClazz, ClassLoader parentClassLoader) {
        this.interfaceClazz = interfaceClazz;
        this.parentClassLoader = parentClassLoader;
        this.cachedClassLoaders = new ConcurrentHashMap<>();
    }

    public O loadClassFromJar(String jarPath, String classFqdn)
            throws ClassNotFoundException, MalformedURLException, InstantiationException, IllegalAccessException {
        ClassLoader classLoader;
        if (isClassLoadedFromParent(classFqdn)) {
            classLoader = parentClassLoader;
        } else {
            classLoader = findCachedClassLoader(jarPath);
            if (classLoader == null) {
                classLoader = getJarAddedClassLoader(jarPath);
            }
        }

        O actualObject = initInstanceFromClassloader(classFqdn, classLoader);
        return createClassLoaderAwareProxyInstance(classLoader, actualObject);
    }

    private ClassLoader findCachedClassLoader(String jarPath) {
        return cachedClassLoaders.get(jarPath);
    }

    private O createClassLoaderAwareProxyInstance(ClassLoader classLoader, O actualObject) {
        InvocationHandler handler = new ClassLoaderAwareInvocationHandler(classLoader, actualObject);
        return (O) Proxy.newProxyInstance(parentClassLoader, new Class<?>[]{interfaceClazz}, handler);
    }

    private O initInstanceFromClassloader(String classFqdn, ClassLoader classLoader) throws ClassNotFoundException,
            InstantiationException, IllegalAccessException {
        Class parserClass = Class.forName(classFqdn, true, classLoader);
        return (O) parserClass.newInstance();
    }

    private ClassLoader getJarAddedClassLoader(String jarPath) throws MalformedURLException {
        ClassLoader classLoader = new MutableURLClassLoader(new URL[0], parentClassLoader);
        URL u = (new File(jarPath).toURI().toURL());
        ((MutableURLClassLoader) classLoader).addURL(u);
        ClassLoader oldCl = cachedClassLoaders.putIfAbsent(jarPath, classLoader);
        if (oldCl != null) {
            // discard and pick old thing
            classLoader = oldCl;
        }
        return classLoader;
    }

    private boolean isClassLoadedFromParent(String className) {
        try {
            Class.forName(className, false, parentClassLoader);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}
