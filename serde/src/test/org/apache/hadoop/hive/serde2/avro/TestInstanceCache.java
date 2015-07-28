/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import java.util.Set;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class TestInstanceCache {
  private static class Foo {

    private int value = 42;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Foo foo = (Foo) o;

      return value == foo.value;

    }

    @Override
    public int hashCode() {
      return value;
    }
  }

  private static class Wrapper<T> {
    public final T wrapped;

    private Wrapper(T wrapped) {
      this.wrapped = wrapped;
    }
  }

  @Test
  public void instanceCachesOnlyCreateOneInstance() throws AvroSerdeException {
    InstanceCache<Foo, Wrapper<Foo>> ic = new InstanceCache<Foo, Wrapper<Foo>>() {
      @Override
      protected Wrapper makeInstance(Foo hv,
                                     Set<Foo> seenSchemas) {
        return new Wrapper(hv);
      }
    };
    Foo f1 = new Foo();

    Wrapper fc = ic.retrieve(f1, null);
    assertSame(f1, fc.wrapped); // Our original foo should be in the wrapper

    Foo f2 = new Foo(); // Different instance, same value

    Wrapper fc2 = ic.retrieve(f2, null);
    assertSame(fc2,fc); // Since equiv f, should get back first container
    assertSame(fc2.wrapped, f1);
  }

  @Test
  public void instanceCacheReturnsCorrectInstances() throws AvroSerdeException {
    InstanceCache<String, Wrapper<String>> ic = new InstanceCache<String, Wrapper<String>>() {
      @Override
      protected Wrapper<String> makeInstance(
          String hv, Set<String> seenSchemas) {
        return new Wrapper<String>(hv);
      }
    };

    Wrapper<String> one = ic.retrieve("one", null);
    Wrapper<String> two = ic.retrieve("two", null);

    Wrapper<String> anotherOne = ic.retrieve("one", null);
    assertSame(one, anotherOne);

    Wrapper<String> anotherTwo = ic.retrieve("two", null);
    assertSame(two, anotherTwo);
  }
}
