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

package org.apache.hadoop.hive.ql.parse;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.antlr.runtime.TokenRewriteStream;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;

/**
 * UnparseTranslator is used to "unparse" objects such as views when their
 * definition is stored.
 */
class UnparseTranslator {
  // key is token start index
  private final NavigableMap<Integer, Translation> translations;
  private boolean enabled;

  public UnparseTranslator() {
    translations = new TreeMap<Integer, Translation>();
  }

  /**
   * Enable this translator.
   */
  void enable() {
    enabled = true;
  }

  /**
   * @return whether this translator has been enabled
   */
  boolean isEnabled() {
    return enabled;
  }

  /**
   * Register a translation to be performed as part of unparse.
   * 
   * @param node
   *          source node whose subtree is to be replaced
   * 
   * @param replacementText
   *          text to use as replacement
   */
  void addTranslation(ASTNode node, String replacementText) {
    if (!enabled) {
      return;
    }

    if (node.getOrigin() != null) {
      // This node was parsed while loading the definition of another view
      // being referenced by the one being created, and we don't want
      // to track any expansions for the underlying view.
      return;
    }

    int tokenStartIndex = node.getTokenStartIndex();
    int tokenStopIndex = node.getTokenStopIndex();

    Translation translation = new Translation();
    translation.tokenStopIndex = tokenStopIndex;
    translation.replacementText = replacementText;

    // Sanity check: no overlap with regions already being expanded
    assert (tokenStopIndex >= tokenStartIndex);
    Map.Entry<Integer, Translation> existingEntry;
    existingEntry = translations.floorEntry(tokenStartIndex);
    if (existingEntry != null) {
      if (existingEntry.getKey() == tokenStartIndex) {
        if (existingEntry.getValue().tokenStopIndex == tokenStopIndex) {
          if (existingEntry.getValue().replacementText.equals(replacementText)) {
            // exact match for existing mapping: somebody is doing something
            // redundant, but we'll let it pass
            return;
          }
        }
      }
      assert (existingEntry.getValue().tokenStopIndex < tokenStartIndex);
    }
    existingEntry = translations.ceilingEntry(tokenStartIndex);
    if (existingEntry != null) {
      assert (existingEntry.getKey() > tokenStopIndex);
    }

    // It's all good: create a new entry in the map
    translations.put(tokenStartIndex, translation);
  }

  /**
   * Register a translation for an identifier.
   * 
   * @param node
   *          source node (which must be an identifier) to be replaced
   */
  void addIdentifierTranslation(ASTNode identifier) {
    if (!enabled) {
      return;
    }
    assert (identifier.getToken().getType() == HiveParser.Identifier);
    String replacementText = identifier.getText();
    replacementText = BaseSemanticAnalyzer.unescapeIdentifier(replacementText);
    replacementText = HiveUtils.unparseIdentifier(replacementText);
    addTranslation(identifier, replacementText);
  }

  /**
   * Apply translations on the given token stream.
   * 
   * @param tokenRewriteStream
   *          rewrite-capable stream
   */
  void applyTranslation(TokenRewriteStream tokenRewriteStream) {
    for (Map.Entry<Integer, Translation> entry : translations.entrySet()) {
      tokenRewriteStream.replace(entry.getKey(),
          entry.getValue().tokenStopIndex, entry.getValue().replacementText);
    }
  }

  private static class Translation {
    int tokenStopIndex;
    String replacementText;

    @Override
    public String toString() {
      return "" + tokenStopIndex + " -> " + replacementText;
    }
  }
}
