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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A red-black tree that stores strings. The strings are stored as UTF-8 bytes
 * and an offset/length for each entry.
 */
class StringRedBlackTree extends RedBlackTree {
  private final DynamicByteArray byteArray = new DynamicByteArray();
  private final DynamicIntArray keySizes = new DynamicIntArray();
  private final Text newKey = new Text();

  public StringRedBlackTree() {
    // PASS
  }

  public StringRedBlackTree(int initialCapacity) {
    super(initialCapacity);
  }

  public int add(String value) {
    newKey.set(value);
    // if the key is new, add it to our byteArray and store the offset & length
    if (add()) {
      int len = newKey.getLength();
      keySizes.add(byteArray.add(newKey.getBytes(), 0, len));
      keySizes.add(len);
    }
    return lastAdd;
  }

  @Override
  protected int compareValue(int position) {
    return byteArray.compare(newKey.getBytes(), 0, newKey.getLength(),
      keySizes.get(2 * position), keySizes.get(2 * position + 1));
  }

  /**
   * The information about each node.
   */
  public interface VisitorContext {
    /**
     * Get the position where the key was originally added.
     * @return the number returned by add.
     */
    int getOriginalPosition();

    /**
     * Write the bytes for the string to the given output stream.
     * @param out the stream to write to.
     * @throws IOException
     */
    void writeBytes(OutputStream out) throws IOException;

    /**
     * Get the original string.
     * @return the string
     */
    Text getText();

    /**
     * Get the number of bytes.
     * @return the string's length in bytes
     */
    int getLength();

    /**
     * Get the count for this key.
     * @return the number of times this key was added
     */
    int getCount();
  }

  /**
   * The interface for visitors.
   */
  public interface Visitor {
    /**
     * Called once for each node of the tree in sort order.
     * @param context the information about each node
     * @throws IOException
     */
    void visit(VisitorContext context) throws IOException;
  }

  private class VisitorContextImpl implements VisitorContext {
    private int originalPosition;
    private final Text text = new Text();

    public int getOriginalPosition() {
      return originalPosition;
    }

    public Text getText() {
      byteArray.setText(text, keySizes.get(originalPosition * 2), getLength());
      return text;
    }

    public void writeBytes(OutputStream out) throws IOException {
      byteArray.write(out, keySizes.get(originalPosition * 2), getLength());
    }

    public int getLength() {
      return keySizes.get(originalPosition * 2 + 1);
    }

    public int getCount() {
      return StringRedBlackTree.this.getCount(originalPosition);
    }
  }

  private void recurse(int node, Visitor visitor, VisitorContextImpl context
                      ) throws IOException {
    if (node != NULL) {
      recurse(getLeft(node), visitor, context);
      context.originalPosition = node;
      visitor.visit(context);
      recurse(getRight(node), visitor, context);
    }
  }

  /**
   * Visit all of the nodes in the tree in sorted order.
   * @param visitor the action to be applied to each ndoe
   * @throws IOException
   */
  public void visit(Visitor visitor) throws IOException {
    recurse(root, visitor, new VisitorContextImpl());
  }

  /**
   * Reset the table to empty.
   */
  public void clear() {
    super.clear();
    byteArray.clear();
    keySizes.clear();
  }

  /**
   * Get the size of the character data in the table.
   * @return the bytes used by the table
   */
  public int getCharacterSize() {
    return byteArray.size();
  }

  /**
   * Calculate the approximate size in memory.
   * @return the number of bytes used in storing the tree.
   */
  public long getByteSize() {
    return byteArray.size() + 5 * 4 * size();
  }
}
