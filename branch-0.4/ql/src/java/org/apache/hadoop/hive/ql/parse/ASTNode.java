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

import java.util.Vector;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.Token;
import org.apache.hadoop.hive.ql.lib.Node;

/**
 * @author athusoo
 *
 */
public class ASTNode extends CommonTree implements Node {

  public ASTNode() {  
  }
  
  /**
   * Constructor
   * @param t Token for the CommonTree Node
   */
  public ASTNode(Token t) {
    super(t);
  }
  
  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.lib.Node#getChildren()
   */
  public Vector<Node> getChildren() {
    if (super.getChildCount() == 0) {
      return null;
    }
    
    Vector<Node> ret_vec = new Vector<Node>();
    for(int i=0; i<super.getChildCount(); ++i) {
      ret_vec.add((Node)super.getChild(i));
    }
    
    return ret_vec;
  }

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.lib.Node#getName()
   */
  public String getName() {
    return (new Integer(super.getToken().getType())).toString();
  }
}
