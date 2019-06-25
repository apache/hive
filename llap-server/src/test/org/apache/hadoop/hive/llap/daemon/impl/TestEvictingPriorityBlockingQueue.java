/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Comparator;

import org.junit.Test;

public class TestEvictingPriorityBlockingQueue {

  @Test (timeout = 10000)
  public void test() throws InterruptedException {
    Element e;
    EvictingPriorityBlockingQueue<Element> queue = new EvictingPriorityBlockingQueue<>(new ElementComparator(), 3);

    Element[] elements = new Element[10];
    for (int i = 0 ; i < elements.length ; i++) {
      elements[i] = new Element(i);
    }

    assertNull(queue.offer(elements[0], 0));
    assertNull(queue.offer(elements[1], 0));
    assertNull(queue.offer(elements[2], 0));
    e = queue.offer(elements[3], 0);
    assertEquals(elements[0], e);

    e = queue.offer(elements[0], 0);
    assertEquals(elements[0], e);
    // 1,2,3

    e = queue.offer(elements[4], 0);
    assertEquals(elements[1], e);
    //2,3,4

    e = queue.offer(elements[1], 1);
    assertNull(e);
    assertEquals(4, queue.size());
    // 1,2,3,4

    e = queue.take();
    assertEquals(elements[4], e); //Highest priority at this point should have come out.
    //1,2,3

    e = queue.offer(elements[4], 1);
    assertNull(e);
    //1,2,3,4

    e = queue.offer(elements[0], 1);
    assertEquals(elements[0], e); // Rejected
    //1,2,3,4

    assertTrue(reinsertIfExists(queue, elements[2]));
    assertEquals(4, queue.size());

    assertFalse(reinsertIfExists(queue, elements[5]));
    assertEquals(4, queue.size());

    //1,2,3,4

    e = queue.offer(elements[5], 1);
    assertEquals(elements[1], e);
    //2,3,4,5

    assertNull(queue.offer(elements[1], 2));

    assertNull(queue.offer(elements[6], 5));
    assertNull(queue.offer(elements[7], 5));
    //1,2,3,4,5,6,7

    assertEquals(7, queue.size());
  }

  /**
   * Test if we are able to set the WaitQueueSize dynamically.
   * @throws InterruptedException
   */
  @Test (timeout = 10000)
  public void testSetWaitQueueSize() throws InterruptedException {
    Element e;
    EvictingPriorityBlockingQueue<Element> queue = new EvictingPriorityBlockingQueue<>(new ElementComparator(), 5);

    Element[] elements = new Element[10];
    for (int i = 0; i < elements.length; i++) {
      elements[i] = new Element(i);
    }

    assertNull(queue.offer(elements[0], 0));
    assertNull(queue.offer(elements[1], 0));
    assertNull(queue.offer(elements[2], 0));
    assertNull(queue.offer(elements[3], 0));
    //0,1,2,3

    queue.setWaitQueueSize(3);

    // Not enough space left in the queue
    e = queue.offer(elements[4], 0);
    assertEquals(e, elements[0]);
    assertEquals(4, queue.size());
    //1,2,3,4

    e = queue.offer(elements[5], 1);
    assertEquals(e, elements[1]);
    assertEquals(4, queue.size());
    //2,3,4,5

    // With 2 more it should be enough space left
    assertNull(queue.offer(elements[6], 2));
    assertEquals(5, queue.size());
    //2,3,4,5,6

    e = queue.take();
    assertEquals(elements[6], e); //Highest priority at this point should have come out.
    //2,3,4,5

    e = queue.take();
    assertEquals(elements[5], e); //Highest priority at this point should have come out.
    //2,3,4

    e = queue.take();
    assertEquals(elements[4], e); //Highest priority at this point should have come out.
    assertEquals(2, queue.size());
    //2,3

    assertNull(queue.offer(elements[7], 0));
    assertEquals(3, queue.size());
    //2,3,7

    e = queue.offer(elements[8], 0);
    assertEquals(e, elements[2]);
    //3,7,8
  }

  private static class Element {
    public Element(int x) {
      this.x = x;
    }

    int x;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Element element = (Element) o;

      return x == element.x;
    }

    @Override
    public int hashCode() {
      return x;
    }
  }

  public static <T> boolean reinsertIfExists(EvictingPriorityBlockingQueue<T> queue, T e) {
    if (queue.remove(e)) {
      queue.forceOffer(e);
      return true;
    }
    return false;
  }

  private static class ElementComparator implements Comparator<Element> {

    @Override
    public int compare(Element o1,
                       Element o2) {
      return o2.x - o1.x;
    }
  }
}
