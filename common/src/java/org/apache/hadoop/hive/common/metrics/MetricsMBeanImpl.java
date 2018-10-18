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
package org.apache.hadoop.hive.common.metrics;

import java.util.HashMap;
import java.util.Map;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InvalidAttributeValueException;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;


public class MetricsMBeanImpl implements  MetricsMBean {

    private final Map<String,Object> metricsMap = new HashMap<String,Object>();

    private MBeanAttributeInfo[] attributeInfos;
    private boolean dirtyAttributeInfoCache = true;

    private static final MBeanConstructorInfo[] ctors = null;
    private static final MBeanOperationInfo[] ops = {new MBeanOperationInfo("reset",
        "Sets the values of all Attributes to 0", null, "void", MBeanOperationInfo.ACTION)};
    private static final MBeanNotificationInfo[] notifs = null;

    @Override
    public Object getAttribute(String arg0) throws AttributeNotFoundException,
            MBeanException, ReflectionException {
      synchronized(metricsMap) {
        if (metricsMap.containsKey(arg0)) {
          return metricsMap.get(arg0);
        } else {
          throw new AttributeNotFoundException("Key [" + arg0 + "] not found/tracked");
        }
      }
    }

    @Override
    public AttributeList getAttributes(String[] arg0) {
      AttributeList results = new AttributeList();
      synchronized(metricsMap) {
        for (String key : arg0) {
          results.add(new Attribute(key,metricsMap.get(key)));
        }
      }
      return results;
    }

    @Override
    public MBeanInfo getMBeanInfo() {
        if (dirtyAttributeInfoCache) {
          synchronized(metricsMap) {
            attributeInfos = new MBeanAttributeInfo[metricsMap.size()];
            int i = 0;
            for (String key : metricsMap.keySet()) {
              attributeInfos[i] = new MBeanAttributeInfo(
                  key, metricsMap.get(key).getClass().getName(), key, true, true/*writable*/, false);
              i++;
            }
            dirtyAttributeInfoCache = false;
          }
        }
        return new MBeanInfo(
            this.getClass().getName(), "metrics information",
            attributeInfos, ctors, ops, notifs);
    }

    @Override
    public Object invoke(String name, Object[] args, String[] signature)
            throws MBeanException, ReflectionException {
        if (name.equals("reset")) {
          reset();
          return null;
        }
        throw new ReflectionException(new NoSuchMethodException(name));
    }

    @Override
    public void setAttribute(Attribute attr) throws AttributeNotFoundException,
            InvalidAttributeValueException, MBeanException, ReflectionException {
        try {
            put(attr.getName(),attr.getValue());
        } catch (Exception e) {
            throw new MBeanException(e);
        }
    }

    @Override
    public AttributeList setAttributes(AttributeList arg0) {
        AttributeList attributesSet = new AttributeList();
        for (Attribute attr : arg0.asList()) {
            try {
                setAttribute(attr);
                attributesSet.add(attr);
            } catch (AttributeNotFoundException e) {
                // ignore exception - we simply don't add this attribute
                // back in to the resultant set.
            } catch (InvalidAttributeValueException e) {
                // ditto
            } catch (MBeanException e) {
                // likewise
            } catch (ReflectionException e) {
                // and again, one last time.
            }
        }
        return attributesSet;
    }

    @Override
    public boolean hasKey(String name) {
      synchronized(metricsMap) {
        return metricsMap.containsKey(name);
      }
    }

    @Override
    public void put(String name, Object value) {
      synchronized(metricsMap) {
        if (!metricsMap.containsKey(name)) {
          dirtyAttributeInfoCache = true;
        }
        metricsMap.put(name, value);
      }
    }

    @Override
    public Object get(String name) throws JMException {
      return getAttribute(name);
    }

    public void reset() {
      synchronized(metricsMap) {
        for (String key : metricsMap.keySet()) {
          metricsMap.put(key, Long.valueOf(0));
        }
      }
    }
    
    @Override
    public void clear() {
      synchronized(metricsMap) {
        attributeInfos = null;
        dirtyAttributeInfoCache = true;
        metricsMap.clear();
      }
    }
}
