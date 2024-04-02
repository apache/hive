/**
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
parser grammar AnalyzeStatementParser;

analyzeStatement
@init { gParent.pushMsg("analyze statement", state); }
@after { gParent.popMsg(state); }
    : KW_ANALYZE KW_TABLE (parttype=tableOrPartition)
      (
      (KW_COMPUTE) => KW_COMPUTE KW_STATISTICS ((noscan=KW_NOSCAN)
                                                      | (KW_FOR KW_COLUMNS (statsColumnName=columnNameList)?))?
      -> ^(TOK_ANALYZE $parttype $noscan? KW_COLUMNS? $statsColumnName?)
      |
      (KW_CACHE) => KW_CACHE KW_METADATA -> ^(TOK_CACHE_METADATA $parttype)
      |
      (KW_DROP) => KW_DROP KW_STATISTICS (KW_FOR KW_COLUMNS (statsColumnName=columnNameList)?) -> ^(TOK_ANALYZE $parttype TOK_DROPSTAT)
      )
    ;
