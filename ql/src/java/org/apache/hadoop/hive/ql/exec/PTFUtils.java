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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;

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
  }
}
