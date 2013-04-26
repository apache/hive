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

package org.apache.hadoop.hive.ql.exec;

import java.beans.BeanInfo;
import java.beans.Encoder;
import java.beans.ExceptionListener;
import java.beans.Expression;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PersistenceDelegate;
import java.beans.PropertyDescriptor;
import java.beans.Statement;
import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.antlr.runtime.CommonToken;
import org.antlr.runtime.tree.BaseTree;
import org.antlr.runtime.tree.CommonTree;
import org.apache.hadoop.hive.ql.exec.Utilities.EnumDelegate;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class PTFUtils {

  public static String toString(List<?> col)
  {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    boolean first = true;
    for (Object o : col)
    {
      if (first) {
        first = false;
      } else {
        buf.append(", ");
      }
      buf.append(o.toString());
    }
    buf.append("]");
    return buf.toString();
  }

  public static String toString(Map<?, ?> col)
  {
    StringBuilder buf = new StringBuilder();
    buf.append("[");
    boolean first = true;
    for (Map.Entry<?, ?> o : col.entrySet())
    {
      if (first) {
        first = false;
      } else {
        buf.append(", ");
      }
      buf.append(o.getKey().toString()).append(" : ")
          .append(o.getValue().toString());
    }
    buf.append("]");
    return buf.toString();
  }

  public static String unescapeQueryString(String qry)
  {
    qry = qry.replace("\\\"", "\"");
    qry = qry.replace("\\'", "'");
    return qry;
  }

  public static class ReverseIterator<T> implements Iterator<T>
  {
    Stack<T> stack;

    public ReverseIterator(Iterator<T> it)
    {
      stack = new Stack<T>();
      while (it.hasNext())
      {
        stack.push(it.next());
      }
    }

    @Override
    public boolean hasNext()
    {
      return !stack.isEmpty();
    }

    @Override
    public T next()
    {
      return stack.pop();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public static abstract class Predicate<T>
  {
    public abstract boolean apply(T obj);
  };



  /*
   * serialization functions
   */
  public static void serialize(OutputStream out, Object o)
  {
    XMLEncoder e = new XMLEncoder(out);
    e.setExceptionListener(new EL());
    PTFUtils.addPersistenceDelegates(e);
    e.writeObject(o);
    e.close();
  }

  public static Object deserialize(InputStream in1)
  {
    XMLDecoder d = null;
    try
    {
      d = new XMLDecoder(in1, null, null);
      return d.readObject();
    }
    finally
    {
      if (null != d)
      {
        d.close();
      }
    }
  }

  public static void addPersistenceDelegates(XMLEncoder e)
  {
    addAntlrPersistenceDelegates(e);
    addHivePersistenceDelegates(e);
    addEnumDelegates(e);
  }

  public static void addEnumDelegates(XMLEncoder e)
  {
    e.setPersistenceDelegate(Direction.class, new EnumDelegate());
  }

  public static void addAntlrPersistenceDelegates(XMLEncoder e)
  {
    e.setPersistenceDelegate(ASTNode.class, new PersistenceDelegate()
    {

      @Override
      protected Expression instantiate(Object oldInstance, Encoder out)
      {
        return new Expression(oldInstance, oldInstance.getClass(),
            "new", new Object[]
            { ((ASTNode) oldInstance).getToken() });
      }
    });
    e.setPersistenceDelegate(CommonTree.class, new PersistenceDelegate()
    {
      @Override
      protected Expression instantiate(Object oldInstance, Encoder out)
      {
        return new Expression(oldInstance, oldInstance.getClass(),
            "new", new Object[]
            { ((CommonTree) oldInstance).getToken() });
      }
    });
    e.setPersistenceDelegate(BaseTree.class, new PersistenceDelegate()
    {
      @Override
      protected Expression instantiate(Object oldInstance, Encoder out)
      {
        return new Expression(oldInstance, oldInstance.getClass(),
            "new", new Object[]
            {});
      }

      @Override
      @SuppressWarnings("rawtypes")
      protected void initialize(Class type, Object oldInstance,
          Object newInstance, Encoder out)
      {
        super.initialize(type, oldInstance, newInstance, out);

        BaseTree t = (BaseTree) oldInstance;

        for (int i = 0; i < t.getChildCount(); i++)
        {
          out.writeStatement(new Statement(oldInstance, "addChild",
              new Object[]
              { t.getChild(i) }));
        }
      }
    });
    e.setPersistenceDelegate(CommonToken.class, new PersistenceDelegate()
    {
      @Override
      protected Expression instantiate(Object oldInstance, Encoder out)
      {
        return new Expression(oldInstance, oldInstance.getClass(),
            "new", new Object[]
            { ((CommonToken) oldInstance).getType(),
                ((CommonToken) oldInstance).getText() });
      }
    });
  }

  public static void addHivePersistenceDelegates(XMLEncoder e)
  {
    e.setPersistenceDelegate(PrimitiveTypeInfo.class,
        new PersistenceDelegate()
        {
          @Override
          protected Expression instantiate(Object oldInstance,
              Encoder out)
          {
            return new Expression(oldInstance,
                TypeInfoFactory.class, "getPrimitiveTypeInfo",
                new Object[]
                { ((PrimitiveTypeInfo) oldInstance)
                    .getTypeName() });
          }
        });
  }

  static class EL implements ExceptionListener
  {
    public void exceptionThrown(Exception e)
    {
      e.printStackTrace();
      throw new RuntimeException("Cannot serialize the query plan", e);
    }
  }

  public static void makeTransient(Class<?> beanClass, String... pdNames) {
    try {
      BeanInfo info = Introspector.getBeanInfo(beanClass);
      PropertyDescriptor[] descs = info.getPropertyDescriptors();
      if (descs == null) {
        throw new RuntimeException("Cannot access property descriptor for class " + beanClass);
      }
      Map<String, PropertyDescriptor> mapping = new HashMap<String, PropertyDescriptor>();
      for (PropertyDescriptor desc : descs) {
        mapping.put(desc.getName(), desc);
      }
      for (String pdName : pdNames) {
        PropertyDescriptor desc = mapping.get(pdName);
        if (desc == null) {
          throw new RuntimeException("Property " + pdName + " does not exist in " + beanClass);
        }
        desc.setValue("transient", Boolean.TRUE);
      }
    }
    catch (IntrospectionException ie) {
      throw new RuntimeException(ie);
    }
  }
}
