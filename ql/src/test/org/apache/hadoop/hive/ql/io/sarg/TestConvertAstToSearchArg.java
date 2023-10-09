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

package org.apache.hadoop.hive.ql.io.sarg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.beans.XMLDecoder;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetFilterPredicateConverter;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument.TruthValue;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * These tests cover the conversion from Hive's AST to SearchArguments.
 */
public class TestConvertAstToSearchArg {

  private final Configuration conf = new Configuration();

  private static void assertNoSharedNodes(ExpressionTree tree,
                                          Set<ExpressionTree> seen
                                          ) throws Exception {
    if (seen.contains(tree) &&
        tree.getOperator() != ExpressionTree.Operator.LEAF) {
      assertTrue("repeated node in expression " + tree, false);
    }
    seen.add(tree);
    if (tree.getChildren() != null) {
      for (ExpressionTree child : tree.getChildren()) {
        assertNoSharedNodes(child, seen);
      }
    }
  }

  private ExprNodeGenericFuncDesc getFuncDesc(String xmlSerialized) {
    byte[] bytes = xmlSerialized.getBytes(StandardCharsets.UTF_8);

    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    XMLDecoder decoder = new XMLDecoder(bais, null, null);

    try {
      return (ExprNodeGenericFuncDesc) decoder.readObject();
    } finally {
      decoder.close();
    }
  }

  @Test
  public void testExpression1() throws Exception {
    // first_name = 'john' or
    //  'greg' < first_name or
    //  'alan' > first_name or
    //  id > 12 or
    //  13 < id or
    //  id < 15 or
    //  16 > id or
    //  (id <=> 30 and first_name <=> 'owen')
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                      <void property=\"children\"> \n" +
        "                       <object class=\"java.util.ArrayList\"> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                          <void property=\"children\"> \n" +
        "                           <object class=\"java.util.ArrayList\"> \n" +
        "                            <void method=\"add\"> \n" +
        "                             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                              <void property=\"children\"> \n" +
        "                               <object class=\"java.util.ArrayList\"> \n" +
        "                                <void method=\"add\"> \n" +
        "                                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                                  <void property=\"column\"> \n" +
        "                                   <string>first_name</string> \n" +
        "                                  </void> \n" +
        "                                  <void property=\"tabAlias\"> \n" +
        "                                   <string>orc_people</string> \n" +
        "                                  </void> \n" +
        "                                  <void property=\"typeInfo\"> \n" +
        "                                   <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                                    <void property=\"typeName\"> \n" +
        "                                     <string>string</string> \n" +
        "                                    </void> \n" +
        "                                   </object> \n" +
        "                                  </void> \n" +
        "                                 </object> \n" +
        "                                </void> \n" +
        "                                <void method=\"add\"> \n" +
        "                                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                                  <void property=\"typeInfo\"> \n" +
        "                                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                                  </void> \n" +
        "                                  <void property=\"value\"> \n" +
        "                                   <string>john</string> \n" +
        "                                  </void> \n" +
        "                                 </object> \n" +
        "                                </void> \n" +
        "                               </object> \n" +
        "                              </void> \n" +
        "                              <void property=\"genericUDF\"> \n" +
        "                               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual\"/> \n" +
        "                              </void> \n" +
        "                              <void property=\"typeInfo\"> \n" +
        "                               <object id=\"PrimitiveTypeInfo1\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                                <void property=\"typeName\"> \n" +
        "                                 <string>boolean</string> \n" +
        "                                </void> \n" +
        "                               </object> \n" +
        "                              </void> \n" +
        "                             </object> \n" +
        "                            </void> \n" +
        "                            <void method=\"add\"> \n" +
        "                             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                              <void property=\"children\"> \n" +
        "                               <object class=\"java.util.ArrayList\"> \n" +
        "                                <void method=\"add\"> \n" +
        "                                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                                  <void property=\"typeInfo\"> \n" +
        "                                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                                  </void> \n" +
        "                                  <void property=\"value\"> \n" +
        "                                   <string>greg</string> \n" +
        "                                  </void> \n" +
        "                                 </object> \n" +
        "                                </void> \n" +
        "                                <void method=\"add\"> \n" +
        "                                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                                  <void property=\"column\"> \n" +
        "                                   <string>first_name</string> \n" +
        "                                  </void> \n" +
        "                                  <void property=\"tabAlias\"> \n" +
        "                                   <string>orc_people</string> \n" +
        "                                  </void> \n" +
        "                                  <void property=\"typeInfo\"> \n" +
        "                                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                                  </void> \n" +
        "                                 </object> \n" +
        "                                </void> \n" +
        "                               </object> \n" +
        "                              </void> \n" +
        "                              <void property=\"genericUDF\"> \n" +
        "                               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "                              </void> \n" +
        "                              <void property=\"typeInfo\"> \n" +
        "                               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                              </void> \n" +
        "                             </object> \n" +
        "                            </void> \n" +
        "                           </object> \n" +
        "                          </void> \n" +
        "                          <void property=\"genericUDF\"> \n" +
        "                           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "                          </void> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                          <void property=\"children\"> \n" +
        "                           <object class=\"java.util.ArrayList\"> \n" +
        "                            <void method=\"add\"> \n" +
        "                             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                              <void property=\"typeInfo\"> \n" +
        "                               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                              </void> \n" +
        "                              <void property=\"value\"> \n" +
        "                               <string>alan</string> \n" +
        "                              </void> \n" +
        "                             </object> \n" +
        "                            </void> \n" +
        "                            <void method=\"add\"> \n" +
        "                             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                              <void property=\"column\"> \n" +
        "                               <string>first_name</string> \n" +
        "                              </void> \n" +
        "                              <void property=\"tabAlias\"> \n" +
        "                               <string>orc_people</string> \n" +
        "                              </void> \n" +
        "                              <void property=\"typeInfo\"> \n" +
        "                               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                              </void> \n" +
        "                             </object> \n" +
        "                            </void> \n" +
        "                           </object> \n" +
        "                          </void> \n" +
        "                          <void property=\"genericUDF\"> \n" +
        "                           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan\"/> \n" +
        "                          </void> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                      <void property=\"genericUDF\"> \n" +
        "                       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                      <void property=\"children\"> \n" +
        "                       <object class=\"java.util.ArrayList\"> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                          <void property=\"column\"> \n" +
        "                           <string>id</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"tabAlias\"> \n" +
        "                           <string>orc_people</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object id=\"PrimitiveTypeInfo2\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                            <void property=\"typeName\"> \n" +
        "                             <string>int</string> \n" +
        "                            </void> \n" +
        "                           </object> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                          </void> \n" +
        "                          <void property=\"value\"> \n" +
        "                           <int>12</int> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                      <void property=\"genericUDF\"> \n" +
        "                       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <int>13</int> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                      <void property=\"column\"> \n" +
        "                       <string>id</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"tabAlias\"> \n" +
        "                       <string>orc_people</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                  <void property=\"column\"> \n" +
        "                   <string>id</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"tabAlias\"> \n" +
        "                   <string>orc_people</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"value\"> \n" +
        "                   <int>15</int> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <int>16</int> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>id</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>id</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <int>30</int> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>first_name</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <string>owen</string> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "  </void> \n" +
        " </object> \n" +
        "</java> \n";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertFalse(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(9, leaves.size());

    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required int32 id;" +
            " required binary first_name; }");

    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("id", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("first_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String[] conditions = new String[]{
      "eq(first_name, Binary{\"john\"})",    /* first_name = 'john' */
      "not(lteq(first_name, Binary{\"greg\"}))", /* 'greg' < first_name */
      "lt(first_name, Binary{\"alan\"})",   /* 'alan' > first_name */
      "not(lteq(id, 12))",                  /* id > 12 or */
      "not(lteq(id, 13))",                  /* 13 < id or */
      "lt(id, 15)",                         /* id < 15 or */
      "lt(id, 16)",                         /* 16 > id or */
      "eq(id, 30)",                         /* id <=> 30 */
      "eq(first_name, Binary{\"owen\"})"    /* first_name <=> 'owen' */
    };
    String expected = String
      .format("and(or(or(or(or(or(or(or(%1$s, %2$s), %3$s), %4$s), %5$s), %6$s), %7$s), %8$s), " +
        "or(or(or(or(or(or(or(%1$s, %2$s), %3$s), %4$s), %5$s), %6$s), %7$s), %9$s))", conditions);
    assertEquals(expected, p.toString());

    PredicateLeaf leaf = leaves.get(0);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
    assertEquals("first_name", leaf.getColumnName());
    assertEquals("john", leaf.getLiteral());

    leaf = leaves.get(1);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN_EQUALS, leaf.getOperator());
    assertEquals("first_name", leaf.getColumnName());
    assertEquals("greg", leaf.getLiteral());

    leaf = leaves.get(2);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("first_name", leaf.getColumnName());
    assertEquals("alan", leaf.getLiteral());

    leaf = leaves.get(3);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN_EQUALS, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(12L, leaf.getLiteral());

    leaf = leaves.get(4);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN_EQUALS, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(13L, leaf.getLiteral());

    leaf = leaves.get(5);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(15L, leaf.getLiteral());

    leaf = leaves.get(6);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(16L, leaf.getLiteral());

    leaf = leaves.get(7);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.NULL_SAFE_EQUALS, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(30L, leaf.getLiteral());

    leaf = leaves.get(8);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.NULL_SAFE_EQUALS, leaf.getOperator());
    assertEquals("first_name", leaf.getColumnName());
    assertEquals("owen", leaf.getLiteral());

    assertEquals("(and (or leaf-0 (not leaf-1) leaf-2 (not leaf-3)" +
        " (not leaf-4) leaf-5 leaf-6 leaf-7)" +
        " (or leaf-0 (not leaf-1) leaf-2 (not leaf-3)" +
        " (not leaf-4) leaf-5 leaf-6 leaf-8))",
        sarg.getExpression().toOldString());
    assertNoSharedNodes(sarg.getExpression(),
        Sets.<ExpressionTree>newIdentityHashSet());
  }

  @Test
  public void testExpression2() throws Exception {
    /* first_name is null or
       first_name <> 'sue' or
       id >= 12 or
       id <= 4; */
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                  <void property=\"column\"> \n" +
        "                   <string>first_name</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"tabAlias\"> \n" +
        "                   <string>orc_people</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                    <void property=\"typeName\"> \n" +
        "                     <string>string</string> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object id=\"PrimitiveTypeInfo1\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                <void property=\"typeName\"> \n" +
        "                 <string>boolean</string> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                  <void property=\"column\"> \n" +
        "                   <string>first_name</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"tabAlias\"> \n" +
        "                   <string>orc_people</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"value\"> \n" +
        "                   <string>sue</string> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>id</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object id=\"PrimitiveTypeInfo2\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                <void property=\"typeName\"> \n" +
        "                 <string>int</string> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <int>12</int> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "          <void property=\"column\"> \n" +
        "           <string>id</string> \n" +
        "          </void> \n" +
        "          <void property=\"tabAlias\"> \n" +
        "           <string>orc_people</string> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <int>4</int> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "  </void> \n" +
        " </object> \n" +
        "</java> \n";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertFalse(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(4, leaves.size());

    String[] conditions = new String[]{
      "eq(first_name, null)",               /* first_name is null  */
      "not(eq(first_name, Binary{\"sue\"}))",    /* first_name <> 'sue' */
      "not(lt(id, 12))",                    /* id >= 12            */
      "lteq(id, 4)"                         /* id <= 4             */
    };

    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required int32 id;" +
            " required binary first_name; }");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("id", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("first_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = String.format("or(or(or(%1$s, %2$s), %3$s), %4$s)", conditions);
    assertEquals(expected, p.toString());

    PredicateLeaf leaf = leaves.get(0);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.IS_NULL, leaf.getOperator());
    assertEquals("first_name", leaf.getColumnName());
    assertEquals(null, leaf.getLiteral());
    assertEquals(null, leaf.getLiteralList());

    leaf = leaves.get(1);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
    assertEquals("first_name", leaf.getColumnName());
    assertEquals("sue", leaf.getLiteral());

    leaf = leaves.get(2);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(12L, leaf.getLiteral());

    leaf = leaves.get(3);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN_EQUALS, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(4L, leaf.getLiteral());

    assertEquals("(or leaf-0 (not leaf-1) (not leaf-2) leaf-3)",
        sarg.getExpression().toOldString());
    assertNoSharedNodes(sarg.getExpression(),
        Sets.<ExpressionTree>newIdentityHashSet());
    assertEquals(TruthValue.NO,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES, TruthValue.YES,
            TruthValue.NO)));
    assertEquals(TruthValue.YES,
        sarg.evaluate(values(TruthValue.YES, TruthValue.YES, TruthValue.YES,
            TruthValue.NO)));
    assertEquals(TruthValue.YES,
        sarg.evaluate(values(TruthValue.NO, TruthValue.NO, TruthValue.YES,
            TruthValue.NO)));
    assertEquals(TruthValue.YES,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES, TruthValue.NO,
            TruthValue.NO)));
    assertEquals(TruthValue.YES,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES, TruthValue.YES,
            TruthValue.YES)));
    assertEquals(TruthValue.NULL,
        sarg.evaluate(values(TruthValue.NULL, TruthValue.YES, TruthValue.YES,
            TruthValue.NO)));
    assertEquals(TruthValue.NULL,
        sarg.evaluate(values(TruthValue.NO, TruthValue.NULL, TruthValue.YES,
            TruthValue.NO)));
    assertEquals(TruthValue.NULL,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES, TruthValue.NULL,
            TruthValue.NO)));
    assertEquals(TruthValue.NULL,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES, TruthValue.YES,
            TruthValue.NULL)));
    assertEquals(TruthValue.YES_NO,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES_NO, TruthValue.YES,
            TruthValue.YES_NO)));
    assertEquals(TruthValue.NO_NULL,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES_NULL, TruthValue.YES,
            TruthValue.NO_NULL)));
    assertEquals(TruthValue.YES_NULL,
        sarg.evaluate(values(TruthValue.YES_NULL, TruthValue.YES_NO_NULL,
            TruthValue.YES, TruthValue.NULL)));
    assertEquals(TruthValue.YES_NO_NULL,
        sarg.evaluate(values(TruthValue.NO_NULL, TruthValue.YES_NO_NULL,
            TruthValue.YES, TruthValue.NO)));
  }

  @Test
  public void testExpression3() throws Exception {
    /* (id between 23 and 45) and
       first_name = 'alan' and
       substr('xxxxx', 3) == first_name and
       'smith' = last_name and
       substr(first_name, 3) == 'yyy' */
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                        <void property=\"typeName\"> \n" +
        "                         <string>boolean</string> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <boolean>false</boolean> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                      <void property=\"column\"> \n" +
        "                       <string>id</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"tabAlias\"> \n" +
        "                       <string>orc_people</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object id=\"PrimitiveTypeInfo1\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                        <void property=\"typeName\"> \n" +
        "                         <string>int</string> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <int>23</int> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <int>45</int> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                      <void property=\"column\"> \n" +
        "                       <string>first_name</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"tabAlias\"> \n" +
        "                       <string>orc_people</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object id=\"PrimitiveTypeInfo2\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                        <void property=\"typeName\"> \n" +
        "                         <string>string</string> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <string>alan</string> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <string>xxxxx</string> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <int>3</int> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge\"> \n" +
        "                    <void property=\"udfClassName\"> \n" +
        "                     <string>org.apache.hadoop.hive.ql.udf.UDFSubstr</string> \n" +
        "                    </void> \n" +
        "                    <void property=\"udfName\"> \n" +
        "                     <string>substr</string> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                  <void property=\"column\"> \n" +
        "                   <string>first_name</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"tabAlias\"> \n" +
        "                   <string>orc_people</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <string>smith</string> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>last_name</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>first_name</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <int>3</int> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge\"> \n" +
        "            <void property=\"udfClassName\"> \n" +
        "             <string>org.apache.hadoop.hive.ql.udf.UDFSubstr</string> \n" +
        "            </void> \n" +
        "            <void property=\"udfName\"> \n" +
        "             <string>substr</string> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <string>yyy</string> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "  </void> \n" +
        " </object> \n" +
        "</java> \n";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertTrue(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(3, leaves.size());

    String[] conditions = new String[]{
      "lteq(id, 45)",                         /* id between 23 and 45 */
      "not(lt(id, 23))",                   /* id between 23 and 45 */
      "eq(first_name, Binary{\"alan\"})",   /* first_name = 'alan'  */
      "eq(last_name, Binary{\"smith\"})"    /* 'smith' = last_name  */
    };
    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required int32 id;" +
            " required binary first_name; required binary last_name;}");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("id", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("first_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));
    columnTypes.put("last_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = String.format("and(and(and(%1$s, %2$s), %3$s), %4$s)", conditions);
    assertEquals(expected, p.toString());

    PredicateLeaf leaf = leaves.get(0);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.BETWEEN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(null, leaf.getLiteral());
    assertEquals(23L, leaf.getLiteralList().get(0));
    assertEquals(45L, leaf.getLiteralList().get(1));

    leaf = leaves.get(1);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
    assertEquals("first_name", leaf.getColumnName());
    assertEquals("alan", leaf.getLiteral());

    leaf = leaves.get(2);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
    assertEquals("last_name", leaf.getColumnName());
    assertEquals("smith", leaf.getLiteral());

    assertEquals("(and leaf-0 leaf-1 leaf-2)",
        sarg.getExpression().toOldString());
    assertNoSharedNodes(sarg.getExpression(),
        Sets.<ExpressionTree>newIdentityHashSet());
  }

  @Test
  public void testExpression4() throws Exception {
    /* id <> 12 and
       first_name in ('john', 'sue') and
       id in (34,50) */
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>id</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                <void property=\"typeName\"> \n" +
        "                 <string>int</string> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <int>12</int> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object id=\"PrimitiveTypeInfo1\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "            <void property=\"typeName\"> \n" +
        "             <string>boolean</string> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>first_name</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object id=\"PrimitiveTypeInfo2\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                <void property=\"typeName\"> \n" +
        "                 <string>string</string> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <string>john</string> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo2\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <string>sue</string> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "          <void property=\"column\"> \n" +
        "           <string>id</string> \n" +
        "          </void> \n" +
        "          <void property=\"tabAlias\"> \n" +
        "           <string>orc_people</string> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <int>34</int> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <int>50</int> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "  </void> \n" +
        " </object> \n" +
        "</java> \n" +
        "\n";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertFalse(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(3, leaves.size());

    String[] conditions = new String[]{
      "not(eq(id, 12))", /* id <> 12 */
      "or(eq(first_name, Binary{\"john\"}), eq(first_name, Binary{\"sue\"}))", /* first_name in
      ('john', 'sue') */
      "or(eq(id, 34), eq(id, 50))" /* id in (34,50) */
    };

    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required int32 id;" +
            " required binary first_name; }");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("id", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("first_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p =
        ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = String.format("and(and(%1$s, %2$s), %3$s)", conditions);
    assertEquals(expected, p.toString());

    PredicateLeaf leaf = leaves.get(0);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(12L, leaf.getLiteral());

    leaf = leaves.get(1);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals(PredicateLeaf.Operator.IN, leaf.getOperator());
    assertEquals("first_name", leaf.getColumnName());
    assertEquals("john", leaf.getLiteralList().get(0));
    assertEquals("sue", leaf.getLiteralList().get(1));

    leaf = leaves.get(2);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.IN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(34L, leaf.getLiteralList().get(0));
    assertEquals(50L, leaf.getLiteralList().get(1));

    assertEquals("(and (not leaf-0) leaf-1 leaf-2)",
        sarg.getExpression().toOldString());
    assertNoSharedNodes(sarg.getExpression(),
        Sets.<ExpressionTree>newIdentityHashSet());
    assertEquals(TruthValue.YES,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES, TruthValue.YES)));
    assertEquals(TruthValue.NULL,
        sarg.evaluate(values(TruthValue.NULL, TruthValue.YES, TruthValue.YES)));
    assertEquals(TruthValue.NULL,
        sarg.evaluate(values(TruthValue.NO, TruthValue.NULL, TruthValue.YES)));
    assertEquals(TruthValue.NO,
        sarg.evaluate(values(TruthValue.YES, TruthValue.YES, TruthValue.YES)));
    assertEquals(TruthValue.NO,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES, TruthValue.NO)));
    assertEquals(TruthValue.NO,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES_NULL, TruthValue.NO)));
    assertEquals(TruthValue.NO_NULL,
        sarg.evaluate(values(TruthValue.NO, TruthValue.NULL, TruthValue.YES_NO_NULL)));
    assertEquals(TruthValue.NO_NULL,
        sarg.evaluate(values(TruthValue.NO, TruthValue.YES, TruthValue.NO_NULL)));
  }

  @Test
  public void testExpression5() throws Exception {
    /* (first_name < 'owen' or 'foobar' = substr(last_name, 4)) and
    first_name between 'david' and 'greg' */
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>first_name</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                <void property=\"typeName\"> \n" +
        "                 <string>string</string> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <string>owen</string> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object id=\"PrimitiveTypeInfo1\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "            <void property=\"typeName\"> \n" +
        "             <string>boolean</string> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <string>foobar</string> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                  <void property=\"column\"> \n" +
        "                   <string>last_name</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"tabAlias\"> \n" +
        "                   <string>orc_people</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                    <void property=\"typeName\"> \n" +
        "                     <string>int</string> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"value\"> \n" +
        "                   <int>4</int> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge\"> \n" +
        "                <void property=\"udfClassName\"> \n" +
        "                 <string>org.apache.hadoop.hive.ql.udf.UDFSubstr</string> \n" +
        "                </void> \n" +
        "                <void property=\"udfName\"> \n" +
        "                 <string>substr</string> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <boolean>false</boolean> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "          <void property=\"column\"> \n" +
        "           <string>first_name</string> \n" +
        "          </void> \n" +
        "          <void property=\"tabAlias\"> \n" +
        "           <string>orc_people</string> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <string>david</string> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <string>greg</string> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "  </void> \n" +
        " </object> \n" +
        "</java> \n";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertTrue(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(1, leaves.size());

    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required int32 id;" +
            " required binary first_name; }");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("id", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("first_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes
        );
    String expected =
      "and(lteq(first_name, Binary{\"greg\"}), not(lt(first_name, Binary{\"david\"})))";
    assertEquals(p.toString(), expected);

    assertEquals(PredicateLeaf.Type.STRING, leaves.get(0).getType());
    assertEquals(PredicateLeaf.Operator.BETWEEN,
        leaves.get(0).getOperator());
    assertEquals("first_name", leaves.get(0).getColumnName());

    assertEquals("leaf-0",
        sarg.getExpression().toOldString());
    assertNoSharedNodes(sarg.getExpression(),
        Sets.<ExpressionTree>newIdentityHashSet());
  }

  @Test
  public void testExpression7() throws Exception {
    /* (id < 10 and id < 11 and id < 12) or (id < 13 and id < 14 and id < 15) or
       (id < 16 and id < 17) or id < 18 */
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                      <void property=\"children\"> \n" +
        "                       <object class=\"java.util.ArrayList\"> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                          <void property=\"column\"> \n" +
        "                           <string>id</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"tabAlias\"> \n" +
        "                           <string>orc_people</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                            <void property=\"typeName\"> \n" +
        "                             <string>int</string> \n" +
        "                            </void> \n" +
        "                           </object> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                          </void> \n" +
        "                          <void property=\"value\"> \n" +
        "                           <int>10</int> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                      <void property=\"genericUDF\"> \n" +
        "                       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object id=\"PrimitiveTypeInfo1\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "                        <void property=\"typeName\"> \n" +
        "                         <string>boolean</string> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                      <void property=\"children\"> \n" +
        "                       <object class=\"java.util.ArrayList\"> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                          <void property=\"column\"> \n" +
        "                           <string>id</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"tabAlias\"> \n" +
        "                           <string>orc_people</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                          </void> \n" +
        "                          <void property=\"value\"> \n" +
        "                           <int>11</int> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                      <void property=\"genericUDF\"> \n" +
        "                       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                      <void property=\"column\"> \n" +
        "                       <string>id</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"tabAlias\"> \n" +
        "                       <string>orc_people</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <int>12</int> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                      <void property=\"children\"> \n" +
        "                       <object class=\"java.util.ArrayList\"> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                          <void property=\"column\"> \n" +
        "                           <string>id</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"tabAlias\"> \n" +
        "                           <string>orc_people</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                          </void> \n" +
        "                          <void property=\"value\"> \n" +
        "                           <int>13</int> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                      <void property=\"genericUDF\"> \n" +
        "                       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                      <void property=\"children\"> \n" +
        "                       <object class=\"java.util.ArrayList\"> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                          <void property=\"column\"> \n" +
        "                           <string>id</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"tabAlias\"> \n" +
        "                           <string>orc_people</string> \n" +
        "                          </void> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                        <void method=\"add\"> \n" +
        "                         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                          <void property=\"typeInfo\"> \n" +
        "                           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                          </void> \n" +
        "                          <void property=\"value\"> \n" +
        "                           <int>14</int> \n" +
        "                          </void> \n" +
        "                         </object> \n" +
        "                        </void> \n" +
        "                       </object> \n" +
        "                      </void> \n" +
        "                      <void property=\"genericUDF\"> \n" +
        "                       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "                  <void property=\"children\"> \n" +
        "                   <object class=\"java.util.ArrayList\"> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                      <void property=\"column\"> \n" +
        "                       <string>id</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"tabAlias\"> \n" +
        "                       <string>orc_people</string> \n" +
        "                      </void> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                    <void method=\"add\"> \n" +
        "                     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                      <void property=\"typeInfo\"> \n" +
        "                       <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                      </void> \n" +
        "                      <void property=\"value\"> \n" +
        "                       <int>15</int> \n" +
        "                      </void> \n" +
        "                     </object> \n" +
        "                    </void> \n" +
        "                   </object> \n" +
        "                  </void> \n" +
        "                  <void property=\"genericUDF\"> \n" +
        "                   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                  <void property=\"column\"> \n" +
        "                   <string>id</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"tabAlias\"> \n" +
        "                   <string>orc_people</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"value\"> \n" +
        "                   <int>16</int> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "              <void property=\"children\"> \n" +
        "               <object class=\"java.util.ArrayList\"> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "                  <void property=\"column\"> \n" +
        "                   <string>id</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"tabAlias\"> \n" +
        "                   <string>orc_people</string> \n" +
        "                  </void> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "                <void method=\"add\"> \n" +
        "                 <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "                  <void property=\"typeInfo\"> \n" +
        "                   <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "                  </void> \n" +
        "                  <void property=\"value\"> \n" +
        "                   <int>17</int> \n" +
        "                  </void> \n" +
        "                 </object> \n" +
        "                </void> \n" +
        "               </object> \n" +
        "              </void> \n" +
        "              <void property=\"genericUDF\"> \n" +
        "               <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "          <void property=\"column\"> \n" +
        "           <string>id</string> \n" +
        "          </void> \n" +
        "          <void property=\"tabAlias\"> \n" +
        "           <string>orc_people</string> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <int>18</int> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "  </void> \n" +
        " </object>\n" +
        "</java>";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertFalse(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(9, leaves.size());

    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required int32 id;" +
            " required binary first_name; }");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("id", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("first_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(and(and(and(and(and(and(and(and(and(and(and(and(and(and(and(and(" +
      "or(or(or(lt(id, 18), lt(id, 10)), lt(id, 13)), lt(id, 16)), " +
      "or(or(or(lt(id, 18), lt(id, 11)), lt(id, 13)), lt(id, 16))), " +
      "or(or(or(lt(id, 18), lt(id, 12)), lt(id, 13)), lt(id, 16))), " +
      "or(or(or(lt(id, 18), lt(id, 10)), lt(id, 14)), lt(id, 16))), " +
      "or(or(or(lt(id, 18), lt(id, 11)), lt(id, 14)), lt(id, 16))), " +
      "or(or(or(lt(id, 18), lt(id, 12)), lt(id, 14)), lt(id, 16))), " +
      "or(or(or(lt(id, 18), lt(id, 10)), lt(id, 15)), lt(id, 16))), " +
      "or(or(or(lt(id, 18), lt(id, 11)), lt(id, 15)), lt(id, 16))), " +
      "or(or(or(lt(id, 18), lt(id, 12)), lt(id, 15)), lt(id, 16))), " +
      "or(or(or(lt(id, 18), lt(id, 10)), lt(id, 13)), lt(id, 17))), " +
      "or(or(or(lt(id, 18), lt(id, 11)), lt(id, 13)), lt(id, 17))), " +
      "or(or(or(lt(id, 18), lt(id, 12)), lt(id, 13)), lt(id, 17))), " +
      "or(or(or(lt(id, 18), lt(id, 10)), lt(id, 14)), lt(id, 17))), " +
      "or(or(or(lt(id, 18), lt(id, 11)), lt(id, 14)), lt(id, 17))), " +
      "or(or(or(lt(id, 18), lt(id, 12)), lt(id, 14)), lt(id, 17))), " +
      "or(or(or(lt(id, 18), lt(id, 10)), lt(id, 15)), lt(id, 17))), " +
      "or(or(or(lt(id, 18), lt(id, 11)), lt(id, 15)), lt(id, 17))), " +
      "or(or(or(lt(id, 18), lt(id, 12)), lt(id, 15)), lt(id, 17)))";
    assertEquals(p.toString(), expected);

    PredicateLeaf leaf = leaves.get(0);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(18L, leaf.getLiteral());

    leaf = leaves.get(1);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(10L, leaf.getLiteral());

    leaf = leaves.get(2);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(13L, leaf.getLiteral());

    leaf = leaves.get(3);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(16L, leaf.getLiteral());

    leaf = leaves.get(4);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(11L, leaf.getLiteral());

    leaf = leaves.get(5);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(12L, leaf.getLiteral());

    leaf = leaves.get(6);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(14L, leaf.getLiteral());

    leaf = leaves.get(7);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(15L, leaf.getLiteral());

    leaf = leaves.get(8);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
    assertEquals("id", leaf.getColumnName());
    assertEquals(17L, leaf.getLiteral());

    assertEquals("(and" +
        " (or leaf-0 leaf-1 leaf-2 leaf-3)" +
        " (or leaf-0 leaf-4 leaf-2 leaf-3)" +
        " (or leaf-0 leaf-5 leaf-2 leaf-3)" +
        " (or leaf-0 leaf-1 leaf-6 leaf-3)" +
        " (or leaf-0 leaf-4 leaf-6 leaf-3)" +
        " (or leaf-0 leaf-5 leaf-6 leaf-3)" +
        " (or leaf-0 leaf-1 leaf-7 leaf-3)" +
        " (or leaf-0 leaf-4 leaf-7 leaf-3)" +
        " (or leaf-0 leaf-5 leaf-7 leaf-3)" +
        " (or leaf-0 leaf-1 leaf-2 leaf-8)" +
        " (or leaf-0 leaf-4 leaf-2 leaf-8)" +
        " (or leaf-0 leaf-5 leaf-2 leaf-8)" +
        " (or leaf-0 leaf-1 leaf-6 leaf-8)" +
        " (or leaf-0 leaf-4 leaf-6 leaf-8)" +
        " (or leaf-0 leaf-5 leaf-6 leaf-8)" +
        " (or leaf-0 leaf-1 leaf-7 leaf-8)" +
        " (or leaf-0 leaf-4 leaf-7 leaf-8)" +
        " (or leaf-0 leaf-5 leaf-7 leaf-8))",
        sarg.getExpression().toOldString());
  }

  @Test
  public void testExpression8() throws Exception {
    /* first_name = last_name */
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "      <void property=\"column\"> \n" +
        "       <string>first_name</string> \n" +
        "      </void> \n" +
        "      <void property=\"tabAlias\"> \n" +
        "       <string>orc_people</string> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "        <void property=\"typeName\"> \n" +
        "         <string>string</string> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "      <void property=\"column\"> \n" +
        "       <string>last_name</string> \n" +
        "      </void> \n" +
        "      <void property=\"tabAlias\"> \n" +
        "       <string>orc_people</string> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "    <void property=\"typeName\"> \n" +
        "     <string>boolean</string> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        " </object> \n" +
        "</java> ";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertTrue(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(0, leaves.size());

    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required int32 id;" +
            " required binary first_name; }");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("id", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("first_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    assertNull(p);

    assertEquals("YES_NO_NULL",
        sarg.getExpression().toString());
  }

  @Test
  public void testExpression9() throws Exception {
    /* first_name = last_name */
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "      <void property=\"column\"> \n" +
        "       <string>id</string> \n" +
        "      </void> \n" +
        "      <void property=\"tabAlias\"> \n" +
        "       <string>orc_people</string> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "        <void property=\"typeName\"> \n" +
        "         <string>int</string> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <int>1</int> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <int>3</int> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge\"> \n" +
        "            <void property=\"operator\"> \n" +
        "             <boolean>true</boolean> \n" +
        "            </void> \n" +
        "            <void property=\"udfClassName\"> \n" +
        "             <string>org.apache.hadoop.hive.ql.udf.UDFOPPlus</string> \n" +
        "            </void> \n" +
        "            <void property=\"udfName\"> \n" +
        "             <string>+</string> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <int>4</int> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge\"> \n" +
        "        <void property=\"operator\"> \n" +
        "         <boolean>true</boolean> \n" +
        "        </void> \n" +
        "        <void property=\"udfClassName\"> \n" +
        "         <string>org.apache.hadoop.hive.ql.udf.UDFOPPlus</string> \n" +
        "        </void> \n" +
        "        <void property=\"udfName\"> \n" +
        "         <string>+</string> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "    <void property=\"typeName\"> \n" +
        "     <string>boolean</string> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        " </object> \n" +
        "</java> ";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertFalse(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(0, leaves.size());

    assertEquals("YES_NO_NULL",
        sarg.getExpression().toString());
    assertEquals(TruthValue.YES_NO_NULL, sarg.evaluate(values()));
  }

  @Test
  public void testExpression10() throws Exception {
    /* id >= 10 and not (10 > id) */
    String exprStr = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
        "<java version=\"1.6.0_31\" class=\"java.beans.XMLDecoder\"> \n" +
        " <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "  <void property=\"children\"> \n" +
        "   <object class=\"java.util.ArrayList\"> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "          <void property=\"column\"> \n" +
        "           <string>id</string> \n" +
        "          </void> \n" +
        "          <void property=\"tabAlias\"> \n" +
        "           <string>orc_people</string> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object id=\"PrimitiveTypeInfo0\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "            <void property=\"typeName\"> \n" +
        "             <string>int</string> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "          </void> \n" +
        "          <void property=\"value\"> \n" +
        "           <int>10</int> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object id=\"PrimitiveTypeInfo1\" class=\"org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo\"> \n" +
        "        <void property=\"typeName\"> \n" +
        "         <string>boolean</string> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "    <void method=\"add\"> \n" +
        "     <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "      <void property=\"children\"> \n" +
        "       <object class=\"java.util.ArrayList\"> \n" +
        "        <void method=\"add\"> \n" +
        "         <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc\"> \n" +
        "          <void property=\"children\"> \n" +
        "           <object class=\"java.util.ArrayList\"> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc\"> \n" +
        "              <void property=\"column\"> \n" +
        "               <string>id</string> \n" +
        "              </void> \n" +
        "              <void property=\"tabAlias\"> \n" +
        "               <string>orc_people</string> \n" +
        "              </void> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "            <void method=\"add\"> \n" +
        "             <object class=\"org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc\"> \n" +
        "              <void property=\"typeInfo\"> \n" +
        "               <object idref=\"PrimitiveTypeInfo0\"/> \n" +
        "              </void> \n" +
        "              <void property=\"value\"> \n" +
        "               <int>10</int> \n" +
        "              </void> \n" +
        "             </object> \n" +
        "            </void> \n" +
        "           </object> \n" +
        "          </void> \n" +
        "          <void property=\"genericUDF\"> \n" +
        "           <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan\"/> \n" +
        "          </void> \n" +
        "          <void property=\"typeInfo\"> \n" +
        "           <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "          </void> \n" +
        "         </object> \n" +
        "        </void> \n" +
        "       </object> \n" +
        "      </void> \n" +
        "      <void property=\"genericUDF\"> \n" +
        "       <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot\"/> \n" +
        "      </void> \n" +
        "      <void property=\"typeInfo\"> \n" +
        "       <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "      </void> \n" +
        "     </object> \n" +
        "    </void> \n" +
        "   </object> \n" +
        "  </void> \n" +
        "  <void property=\"genericUDF\"> \n" +
        "   <object class=\"org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd\"/> \n" +
        "  </void> \n" +
        "  <void property=\"typeInfo\"> \n" +
        "   <object idref=\"PrimitiveTypeInfo1\"/> \n" +
        "  </void> \n" +
        " </object> \n" +
        "</java>";

    ConvertAstToSearchArg.Result result = ConvertAstToSearchArg.createSearchArgument(conf, getFuncDesc(exprStr));
    assertFalse(result.isPartial());
    SearchArgumentImpl sarg =
        (SearchArgumentImpl) result.getSearchArgument();
    List<PredicateLeaf> leaves = sarg.getLeaves();
    assertEquals(1, leaves.size());

    MessageType schema =
        MessageTypeParser.parseMessageType("message test { required int32 id;" +
            " required binary first_name; }");
    Map<String, TypeInfo> columnTypes = new HashMap<>();
    columnTypes.put("id", TypeInfoFactory.getPrimitiveTypeInfo("int"));
    columnTypes.put("first_name", TypeInfoFactory.getPrimitiveTypeInfo("string"));

    FilterPredicate p = ParquetFilterPredicateConverter.toFilterPredicate(sarg, schema, columnTypes);
    String expected = "and(not(lt(id, 10)), not(lt(id, 10)))";
    assertEquals(expected, p.toString());

    assertEquals(PredicateLeaf.Type.LONG, leaves.get(0).getType());
    assertEquals(PredicateLeaf.Operator.LESS_THAN,
        leaves.get(0).getOperator());
    assertEquals("id", leaves.get(0).getColumnName());
    assertEquals(10L, leaves.get(0).getLiteral());

    assertEquals("(and (not leaf-0) (not leaf-0))",
        sarg.getExpression().toOldString());
    assertNoSharedNodes(sarg.getExpression(),
        Sets.<ExpressionTree>newIdentityHashSet());
    assertEquals(TruthValue.NO, sarg.evaluate(values(TruthValue.YES)));
    assertEquals(TruthValue.YES, sarg.evaluate(values(TruthValue.NO)));
    assertEquals(TruthValue.NULL, sarg.evaluate(values(TruthValue.NULL)));
    assertEquals(TruthValue.NO_NULL, sarg.evaluate(values(TruthValue.YES_NULL)));
    assertEquals(TruthValue.YES_NULL, sarg.evaluate(values(TruthValue.NO_NULL)));
    assertEquals(TruthValue.YES_NO, sarg.evaluate(values(TruthValue.YES_NO)));
    assertEquals(TruthValue.YES_NO_NULL, sarg.evaluate(values(TruthValue.YES_NO_NULL)));
  }

  private static TruthValue[] values(TruthValue... vals) {
    return vals;
  }

  @Test
  public void testTimestampSarg() throws Exception {
    ExprNodeGenericFuncDesc node = getColumnEqualsConstantExpression(
        TypeInfoFactory.timestampTypeInfo, "ts", Timestamp.ofEpochMilli(1426595696000L));
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();
    assertEquals("leaf-0", sarg.getExpression().toOldString());
    assertEquals(1, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.TIMESTAMP, leaf.getType());
    assertEquals("(EQUALS ts 2015-03-17 05:34:56.0)", leaf.toString());
  }

  @Test
  public void testDateSarg() throws Exception {
    ExprNodeGenericFuncDesc node =
        getColumnEqualsConstantExpression(TypeInfoFactory.dateTypeInfo, "dt", "2015-05-05");
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();
    assertEquals("leaf-0", sarg.getExpression().toOldString());
    assertEquals(1, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.DATE, leaf.getType());
    assertEquals("(EQUALS dt 2015-05-05)", leaf.toString());
  }

  @Test
  public void testDecimalSarg() throws Exception {
    ExprNodeGenericFuncDesc node =
        getColumnEqualsConstantExpression(TypeInfoFactory.decimalTypeInfo, "dec", 123);
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();
    assertEquals("leaf-0", sarg.getExpression().toOldString());
    assertEquals(1, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.DECIMAL, leaf.getType());
    assertEquals("(EQUALS dec 123)", leaf.toString());
  }

  @Test
  public void testCharSarg() throws Exception {
    ExprNodeGenericFuncDesc node =
        getColumnEqualsConstantExpression(TypeInfoFactory.charTypeInfo, "ch", "char      ");
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();
    assertEquals("leaf-0", sarg.getExpression().toOldString());
    assertEquals(1, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals("(EQUALS ch char      )", leaf.toString());
  }

  @Test
  public void testVarcharSarg() throws Exception {
    ExprNodeGenericFuncDesc node =
        getColumnEqualsConstantExpression(TypeInfoFactory.varcharTypeInfo, "vc", "variable");
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();;
    assertEquals("leaf-0", sarg.getExpression().toOldString());
    assertEquals(1, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
    assertEquals("(EQUALS vc variable)", leaf.toString());
  }

  @Test
  public void testBigintSarg() throws Exception {
    ExprNodeGenericFuncDesc node =
        getColumnEqualsConstantExpression(TypeInfoFactory.intTypeInfo, "bi", 12345);
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();
    assertEquals("leaf-0", sarg.getExpression().toOldString());
    assertEquals(1, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    assertEquals("(EQUALS bi 12345)", leaf.toString());
  }

  @Test
  public void testBooleanSarg() throws Exception {
    ExprNodeDesc column1 =
        new ExprNodeColumnDesc(TypeInfoFactory.booleanTypeInfo, "b1", null, false);
    ExprNodeDesc column2 =
        new ExprNodeColumnDesc(TypeInfoFactory.booleanTypeInfo, "b2", null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true);

    ExprNodeGenericFuncDesc function1 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(), Lists.newArrayList(column1, constant));
    ExprNodeGenericFuncDesc function2 = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(), Lists.newArrayList(column2, constant));

    List<ExprNodeDesc> children = Lists.newArrayList();

    children.add(function1);
    children.add(function2);
    ExprNodeGenericFuncDesc node = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPAnd(), children);
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();
    assertEquals("(and leaf-0 leaf-1)", sarg.getExpression().toOldString());
    assertEquals(2, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.BOOLEAN, leaf.getType());
    assertEquals("(EQUALS b1 true)", leaf.toString());
    leaf = sarg.getLeaves().get(1);
    assertEquals(PredicateLeaf.Type.BOOLEAN, leaf.getType());
    assertEquals("(EQUALS b2 true)", leaf.toString());
  }

  @Test
  public void testFloatSarg() throws Exception {
    ExprNodeGenericFuncDesc node =
        getColumnEqualsConstantExpression(TypeInfoFactory.floatTypeInfo, "flt", 1.1f);
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();
    assertEquals("leaf-0", sarg.getExpression().toOldString());
    assertEquals(1, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.FLOAT, leaf.getType());
    assertEquals("(EQUALS flt " + ((Number) 1.1f).doubleValue() + ")", leaf.toString());
  }

  @Test
  public void testDoubleSarg() throws Exception {
    ExprNodeGenericFuncDesc node =
        getColumnEqualsConstantExpression(TypeInfoFactory.doubleTypeInfo, "dbl", 2.2);
    String serialAst = SerializationUtilities.serializeExpression(node);

    ConvertAstToSearchArg convertAstToSearchArg = new ConvertAstToSearchArg(conf, SerializationUtilities.deserializeExpression(serialAst));
    assertFalse(convertAstToSearchArg.isPartial());
    SearchArgument sarg = convertAstToSearchArg.buildSearchArgument();
    assertEquals("leaf-0", sarg.getExpression().toOldString());
    assertEquals(1, sarg.getLeaves().size());
    PredicateLeaf leaf = sarg.getLeaves().get(0);
    assertEquals(PredicateLeaf.Type.FLOAT, leaf.getType());
    assertEquals("(EQUALS dbl 2.2)", leaf.toString());
  }

  private ExprNodeGenericFuncDesc getColumnEqualsConstantExpression(TypeInfo typeInfo,
      String columnName, Object value) {
    ExprNodeDesc column = new ExprNodeColumnDesc(typeInfo, columnName, null, false);
    ExprNodeDesc constant = new ExprNodeConstantDesc(typeInfo, value);
    List<ExprNodeDesc> children = Lists.newArrayList();
    children.add(column);
    children.add(constant);
    ExprNodeGenericFuncDesc node =
        new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPEqual(), children);
    return node;
  }
}
