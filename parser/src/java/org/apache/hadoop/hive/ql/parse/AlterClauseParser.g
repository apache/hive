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
parser grammar AlterClauseParser;

options
{
output=AST;
ASTLabelType=ASTNode;
backtrack=false;
k=3;
}

@members {
  @Override
  public Object recoverFromMismatchedSet(IntStream input,
      RecognitionException re, BitSet follow) throws RecognitionException {
    throw re;
  }
  @Override
  public void displayRecognitionError(String[] tokenNames,
      RecognitionException e) {
    gParent.errors.add(new ParseError(gParent, e, tokenNames));
  }
}

@rulecatch {
catch (RecognitionException e) {
  throw e;
}
}


alterStatement
@init { gParent.pushMsg("alter statement", state); }
@after { gParent.popMsg(state); }
    : KW_ALTER KW_TABLE tableName alterTableStatementSuffix -> ^(TOK_ALTERTABLE tableName alterTableStatementSuffix)
    | KW_ALTER KW_VIEW tableName KW_AS? alterViewStatementSuffix -> ^(TOK_ALTERVIEW tableName alterViewStatementSuffix)
    | KW_ALTER KW_MATERIALIZED KW_VIEW tableNameTree=tableName alterMaterializedViewStatementSuffix[$tableNameTree.tree] -> alterMaterializedViewStatementSuffix
    | KW_ALTER (KW_DATABASE|KW_SCHEMA) alterDatabaseStatementSuffix -> alterDatabaseStatementSuffix
    | KW_ALTER KW_DATACONNECTOR alterDataConnectorStatementSuffix -> alterDataConnectorStatementSuffix
    ;

alterTableStatementSuffix
@init { gParent.pushMsg("alter table statement", state); }
@after { gParent.popMsg(state); }
    : (alterStatementSuffixRename[true]) => alterStatementSuffixRename[true]
    | alterStatementSuffixDropPartitions[true]
    | alterStatementSuffixAddPartitions[true]
    | alterStatementSuffixTouch
    | alterStatementSuffixArchive
    | alterStatementSuffixUnArchive
    | alterStatementSuffixProperties
    | alterStatementSuffixSkewedby
    | alterStatementSuffixExchangePartition
    | alterStatementPartitionKeyType
    | alterStatementSuffixDropConstraint
    | alterStatementSuffixAddConstraint
    | alterTblPartitionStatementSuffix[false]
    | partitionSpec alterTblPartitionStatementSuffix[true] -> alterTblPartitionStatementSuffix partitionSpec
    | alterStatementSuffixSetOwner
    | alterStatementSuffixSetPartSpec
    ;

alterTblPartitionStatementSuffix[boolean partition]
@init {gParent.pushMsg("alter table partition statement suffix", state);}
@after {gParent.popMsg(state);}
  : alterStatementSuffixFileFormat[partition]
  | alterStatementSuffixLocation[partition]
  | alterStatementSuffixMergeFiles[partition]
  | alterStatementSuffixSerdeProperties[partition]
  | alterStatementSuffixRenamePart
  | alterStatementSuffixBucketNum[partition]
  | alterTblPartitionStatementSuffixSkewedLocation
  | alterStatementSuffixClusterbySortby
  | alterStatementSuffixCompact
  | alterStatementSuffixUpdateStatsCol[partition]
  | alterStatementSuffixUpdateStats[partition]
  | alterStatementSuffixRenameCol
  | alterStatementSuffixAddCol
  | alterStatementSuffixUpdateColumns
  ;

alterStatementPartitionKeyType
@init {gParent.msgs.push("alter partition key type"); }
@after {gParent.msgs.pop();}
	: KW_PARTITION KW_COLUMN LPAREN columnNameType RPAREN
	-> ^(TOK_ALTERTABLE_PARTCOLTYPE columnNameType)
	;

alterViewStatementSuffix
@init { gParent.pushMsg("alter view statement", state); }
@after { gParent.popMsg(state); }
    : alterViewSuffixProperties
    | alterStatementSuffixRename[false]
    | alterStatementSuffixAddPartitions[false]
    | alterStatementSuffixDropPartitions[false]
    | selectStatementWithCTE -> ^(TOK_ALTERVIEW_AS selectStatementWithCTE)
    ;

alterMaterializedViewStatementSuffix[CommonTree tableNameTree]
@init { gParent.pushMsg("alter materialized view statement", state); }
@after { gParent.popMsg(state); }
    : alterMaterializedViewSuffixRewrite[tableNameTree]
    | alterMaterializedViewSuffixRebuild[tableNameTree]
    ;

alterMaterializedViewSuffixRewrite[CommonTree tableNameTree]
@init { gParent.pushMsg("alter materialized view rewrite statement", state); }
@after { gParent.popMsg(state); }
    : (mvRewriteFlag=rewriteEnabled | mvRewriteFlag=rewriteDisabled)
    -> ^(TOK_ALTER_MATERIALIZED_VIEW_REWRITE {$tableNameTree} $mvRewriteFlag)
    ;

alterMaterializedViewSuffixRebuild[CommonTree tableNameTree]
@init { gParent.pushMsg("alter materialized view rebuild statement", state); }
@after { gParent.popMsg(state); }
    : KW_REBUILD -> ^(TOK_ALTER_MATERIALIZED_VIEW_REBUILD {$tableNameTree})
    ;

alterDatabaseStatementSuffix
@init { gParent.pushMsg("alter database statement", state); }
@after { gParent.popMsg(state); }
    : alterDatabaseSuffixProperties
    | alterDatabaseSuffixSetOwner
    | alterDatabaseSuffixSetLocation
    ;

alterDatabaseSuffixProperties
@init { gParent.pushMsg("alter database properties statement", state); }
@after { gParent.popMsg(state); }
    : name=identifier KW_SET KW_DBPROPERTIES dbProperties
    -> ^(TOK_ALTERDATABASE_PROPERTIES $name dbProperties)
    ;

alterDatabaseSuffixSetOwner
@init { gParent.pushMsg("alter database set owner", state); }
@after { gParent.popMsg(state); }
    : dbName=identifier KW_SET KW_OWNER principalName
    -> ^(TOK_ALTERDATABASE_OWNER $dbName principalName)
    ;

alterDatabaseSuffixSetLocation
@init { gParent.pushMsg("alter database set location", state); }
@after { gParent.popMsg(state); }
    : dbName=identifier KW_SET KW_LOCATION newLocation=StringLiteral
    -> ^(TOK_ALTERDATABASE_LOCATION $dbName $newLocation)
    | dbName=identifier KW_SET KW_MANAGEDLOCATION newLocation=StringLiteral
    -> ^(TOK_ALTERDATABASE_MANAGEDLOCATION $dbName $newLocation)
    ;

alterDatabaseSuffixSetManagedLocation
@init { gParent.pushMsg("alter database set managed location", state); }
@after { gParent.popMsg(state); }
    : dbName=identifier KW_SET KW_MANAGEDLOCATION newLocation=StringLiteral
    -> ^(TOK_ALTERDATABASE_MANAGEDLOCATION $dbName $newLocation)
    ;

alterStatementSuffixRename[boolean table]
@init { gParent.pushMsg("rename statement", state); }
@after { gParent.popMsg(state); }
    : KW_RENAME KW_TO tableName
    -> { table }? ^(TOK_ALTERTABLE_RENAME tableName)
    ->            ^(TOK_ALTERVIEW_RENAME tableName)
    ;

alterStatementSuffixAddCol
@init { gParent.pushMsg("add column statement", state); }
@after { gParent.popMsg(state); }
    : (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN restrictOrCascade?
    -> {$add != null}? ^(TOK_ALTERTABLE_ADDCOLS columnNameTypeList restrictOrCascade?)
    ->                 ^(TOK_ALTERTABLE_REPLACECOLS columnNameTypeList restrictOrCascade?)
    ;

alterStatementSuffixAddConstraint
@init { gParent.pushMsg("add constraint statement", state); }
@after { gParent.popMsg(state); }
   :  KW_ADD (fk=alterForeignKeyWithName | alterConstraintWithName)
   -> {fk != null}? ^(TOK_ALTERTABLE_ADDCONSTRAINT alterForeignKeyWithName)
   ->               ^(TOK_ALTERTABLE_ADDCONSTRAINT alterConstraintWithName)
   ;

alterStatementSuffixUpdateColumns
@init { gParent.pushMsg("update columns statement", state); }
@after { gParent.popMsg(state); }
    : KW_UPDATE KW_COLUMNS restrictOrCascade?
    -> ^(TOK_ALTERTABLE_UPDATECOLUMNS restrictOrCascade?)
    ;

alterStatementSuffixDropConstraint
@init { gParent.pushMsg("drop constraint statement", state); }
@after { gParent.popMsg(state); }
   : KW_DROP KW_CONSTRAINT cName=identifier
   ->^(TOK_ALTERTABLE_DROPCONSTRAINT $cName)
   ;

alterStatementSuffixRenameCol
@init { gParent.pushMsg("rename column name", state); }
@after { gParent.popMsg(state); }
    : KW_CHANGE KW_COLUMN? oldName=identifier newName=identifier colType alterColumnConstraint[$newName.tree]? (KW_COMMENT comment=StringLiteral)? alterStatementChangeColPosition? restrictOrCascade?
    ->^(TOK_ALTERTABLE_RENAMECOL $oldName $newName colType $comment? alterColumnConstraint? alterStatementChangeColPosition? restrictOrCascade?)
    ;

alterStatementSuffixUpdateStatsCol[boolean partition]
@init { gParent.pushMsg("update column statistics", state); }
@after { gParent.popMsg(state); }
    : KW_UPDATE KW_STATISTICS KW_FOR KW_COLUMN? colName=identifier KW_SET tableProperties (KW_COMMENT comment=StringLiteral)?
    -> {partition}? ^(TOK_ALTERPARTITION_UPDATECOLSTATS $colName tableProperties $comment?)
    ->              ^(TOK_ALTERTABLE_UPDATECOLSTATS $colName tableProperties $comment?)
    ;

alterStatementSuffixUpdateStats[boolean partition]
@init { gParent.pushMsg("update basic statistics", state); }
@after { gParent.popMsg(state); }
    : KW_UPDATE KW_STATISTICS KW_SET tableProperties
    -> {partition}? ^(TOK_ALTERPARTITION_UPDATESTATS tableProperties)
    ->              ^(TOK_ALTERTABLE_UPDATESTATS tableProperties)
    ;

alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=identifier
    ->{$first != null}? ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION )
    -> ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION $afterCol)
    ;

alterStatementSuffixAddPartitions[boolean table]
@init { gParent.pushMsg("add partition statement", state); }
@after { gParent.popMsg(state); }
    : KW_ADD ifNotExists? alterStatementSuffixAddPartitionsElement+
    -> { table }? ^(TOK_ALTERTABLE_ADDPARTS ifNotExists? alterStatementSuffixAddPartitionsElement+)
    ->            ^(TOK_ALTERVIEW_ADDPARTS ifNotExists? alterStatementSuffixAddPartitionsElement+)
    ;

alterStatementSuffixAddPartitionsElement
    : partitionSpec partitionLocation?
    ;

alterStatementSuffixTouch
@init { gParent.pushMsg("touch statement", state); }
@after { gParent.popMsg(state); }
    : KW_TOUCH (partitionSpec)*
    -> ^(TOK_ALTERTABLE_TOUCH (partitionSpec)*)
    ;

alterStatementSuffixArchive
@init { gParent.pushMsg("archive statement", state); }
@after { gParent.popMsg(state); }
    : KW_ARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_ARCHIVE (partitionSpec)*)
    ;

alterStatementSuffixUnArchive
@init { gParent.pushMsg("unarchive statement", state); }
@after { gParent.popMsg(state); }
    : KW_UNARCHIVE (partitionSpec)*
    -> ^(TOK_ALTERTABLE_UNARCHIVE (partitionSpec)*)
    ;

partitionLocation
@init { gParent.pushMsg("partition location", state); }
@after { gParent.popMsg(state); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_PARTITIONLOCATION $locn)
    ;

alterStatementSuffixDropPartitions[boolean table]
@init { gParent.pushMsg("drop partition statement", state); }
@after { gParent.popMsg(state); }
    : KW_DROP ifExists? KW_PARTITION partitionSelectorSpec (COMMA KW_PARTITION partitionSelectorSpec)* KW_PURGE? replicationClause?
    -> { table }? ^(TOK_ALTERTABLE_DROPPARTS partitionSelectorSpec+ ifExists? KW_PURGE? replicationClause?)
    ->            ^(TOK_ALTERVIEW_DROPPARTS partitionSelectorSpec+ ifExists? replicationClause?)
    ;

alterStatementSuffixProperties
@init { gParent.pushMsg("alter properties statement", state); }
@after { gParent.popMsg(state); }
    : KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_PROPERTIES tableProperties)
    | KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    -> ^(TOK_ALTERTABLE_DROPPROPERTIES tableProperties ifExists?)
    ;

alterViewSuffixProperties
@init { gParent.pushMsg("alter view properties statement", state); }
@after { gParent.popMsg(state); }
    : KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERVIEW_PROPERTIES tableProperties)
    | KW_UNSET KW_TBLPROPERTIES ifExists? tableProperties
    -> ^(TOK_ALTERVIEW_DROPPROPERTIES tableProperties ifExists?)
    ;

alterStatementSuffixSerdeProperties[boolean partition]
@init { gParent.pushMsg("alter serde statement", state); }
@after { gParent.popMsg(state); }
    : KW_SET KW_SERDE serdeName=StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    -> {partition}? ^(TOK_ALTERPARTITION_SERIALIZER $serdeName tableProperties?)
    ->              ^(TOK_ALTERTABLE_SERIALIZER $serdeName tableProperties?)
    | KW_SET KW_SERDEPROPERTIES tableProperties
    -> {partition}? ^(TOK_ALTERPARTITION_SETSERDEPROPERTIES tableProperties)
    ->              ^(TOK_ALTERTABLE_SETSERDEPROPERTIES tableProperties)
    | KW_UNSET KW_SERDEPROPERTIES tableProperties
    -> {partition}? ^(TOK_ALTERPARTITION_UNSETSERDEPROPERTIES tableProperties)
    ->              ^(TOK_ALTERTABLE_UNSETSERDEPROPERTIES tableProperties)
    ;

tablePartitionPrefix
@init {gParent.pushMsg("table partition prefix", state);}
@after {gParent.popMsg(state);}
  : tableName partitionSpec?
  ->^(TOK_TABLE_PARTITION tableName partitionSpec?)
  ;

alterStatementSuffixFileFormat[boolean partition]
@init {gParent.pushMsg("alter fileformat statement", state); }
@after {gParent.popMsg(state);}
  : KW_SET KW_FILEFORMAT fileFormat
  -> {partition}? ^(TOK_ALTERPARTITION_FILEFORMAT fileFormat)
  ->              ^(TOK_ALTERTABLE_FILEFORMAT fileFormat)
  ;

alterStatementSuffixClusterbySortby
@init {gParent.pushMsg("alter partition cluster by sort by statement", state);}
@after {gParent.popMsg(state);}
  : KW_NOT KW_CLUSTERED -> ^(TOK_ALTERTABLE_CLUSTER_SORT TOK_NOT_CLUSTERED)
  | KW_NOT KW_SORTED -> ^(TOK_ALTERTABLE_CLUSTER_SORT TOK_NOT_SORTED)
  | tableBuckets -> ^(TOK_ALTERTABLE_CLUSTER_SORT tableBuckets)
  ;

alterTblPartitionStatementSuffixSkewedLocation
@init {gParent.pushMsg("alter partition skewed location", state);}
@after {gParent.popMsg(state);}
  : KW_SET KW_SKEWED KW_LOCATION skewedLocations
  -> ^(TOK_ALTERTABLE_SKEWED_LOCATION skewedLocations)
  ;

skewedLocations
@init { gParent.pushMsg("skewed locations", state); }
@after { gParent.popMsg(state); }
    :
      LPAREN skewedLocationsList RPAREN -> ^(TOK_SKEWED_LOCATIONS skewedLocationsList)
    ;

skewedLocationsList
@init { gParent.pushMsg("skewed locations list", state); }
@after { gParent.popMsg(state); }
    :
      skewedLocationMap (COMMA skewedLocationMap)* -> ^(TOK_SKEWED_LOCATION_LIST skewedLocationMap+)
    ;

skewedLocationMap
@init { gParent.pushMsg("specifying skewed location map", state); }
@after { gParent.popMsg(state); }
    :
      key=skewedValueLocationElement EQUAL value=StringLiteral -> ^(TOK_SKEWED_LOCATION_MAP $key $value)
    ;

alterStatementSuffixLocation[boolean partition]
@init {gParent.pushMsg("alter location", state);}
@after {gParent.popMsg(state);}
  : KW_SET KW_LOCATION newLoc=StringLiteral
  -> {partition}? ^(TOK_ALTERPARTITION_LOCATION $newLoc)
  ->              ^(TOK_ALTERTABLE_LOCATION $newLoc)
  ;


alterStatementSuffixSkewedby
@init {gParent.pushMsg("alter skewed by statement", state);}
@after{gParent.popMsg(state);}
	: tableSkewed
	->^(TOK_ALTERTABLE_SKEWED tableSkewed)
	|
	 KW_NOT KW_SKEWED
	->^(TOK_ALTERTABLE_SKEWED)
	|
	 KW_NOT storedAsDirs
	->^(TOK_ALTERTABLE_SKEWED storedAsDirs)
	;

alterStatementSuffixExchangePartition
@init {gParent.pushMsg("alter exchange partition", state);}
@after{gParent.popMsg(state);}
    : KW_EXCHANGE partitionSpec KW_WITH KW_TABLE exchangename=tableName
    -> ^(TOK_ALTERTABLE_EXCHANGEPARTITION partitionSpec $exchangename)
    ;

alterStatementSuffixRenamePart
@init { gParent.pushMsg("alter table rename partition statement", state); }
@after { gParent.popMsg(state); }
    : KW_RENAME KW_TO partitionSpec
    ->^(TOK_ALTERTABLE_RENAMEPART partitionSpec)
    ;

alterStatementSuffixStatsPart
@init { gParent.pushMsg("alter table stats partition statement", state); }
@after { gParent.popMsg(state); }
    : KW_UPDATE KW_STATISTICS KW_FOR KW_COLUMN? colName=identifier KW_SET tableProperties (KW_COMMENT comment=StringLiteral)?
    ->^(TOK_ALTERTABLE_UPDATECOLSTATS $colName tableProperties $comment?)
    ;

alterStatementSuffixMergeFiles[boolean partition]
@init { gParent.pushMsg("", state); }
@after { gParent.popMsg(state); }
    : KW_CONCATENATE
    -> {partition}? ^(TOK_ALTERPARTITION_MERGEFILES)
    ->              ^(TOK_ALTERTABLE_MERGEFILES)
    ;

alterStatementSuffixBucketNum[boolean partition]
@init { gParent.pushMsg("", state); }
@after { gParent.popMsg(state); }
    : KW_INTO num=Number KW_BUCKETS
    -> {partition}? ^(TOK_ALTERPARTITION_BUCKETS $num)
    ->              ^(TOK_ALTERTABLE_BUCKETS $num)
    ;

blocking
  : KW_AND KW_WAIT
  -> TOK_BLOCKING
  ;

alterStatementSuffixCompact
@init { gParent.msgs.push("compaction request"); }
@after { gParent.msgs.pop(); }
    : KW_COMPACT compactType=StringLiteral blocking? (KW_WITH KW_OVERWRITE KW_TBLPROPERTIES tableProperties)?
    -> ^(TOK_ALTERTABLE_COMPACT $compactType blocking? tableProperties?)
    ;

alterStatementSuffixSetOwner
@init { gParent.pushMsg("alter table set owner", state); }
@after { gParent.popMsg(state); }
    : KW_SET KW_OWNER principalName
    -> ^(TOK_ALTERTABLE_OWNER principalName)
    ;

alterStatementSuffixSetPartSpec
@init { gParent.pushMsg("alter table set partition spec", state); }
@after { gParent.popMsg(state); }
    : KW_SET KW_PARTITION KW_SPEC LPAREN (spec = partitionTransformSpec) RPAREN
    -> ^(TOK_ALTERTABLE_SETPARTSPEC $spec)
    ;

fileFormat
@init { gParent.pushMsg("file format specification", state); }
@after { gParent.popMsg(state); }
    : KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral KW_SERDE serdeCls=StringLiteral (KW_INPUTDRIVER inDriver=StringLiteral KW_OUTPUTDRIVER outDriver=StringLiteral)?
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt $serdeCls $inDriver? $outDriver?)
    | genericSpec=identifier -> ^(TOK_FILEFORMAT_GENERIC $genericSpec)
    ;

alterDataConnectorStatementSuffix
@init { gParent.pushMsg("alter connector statement", state); }
@after { gParent.popMsg(state); }
    : alterDataConnectorSuffixProperties
    | alterDataConnectorSuffixSetOwner
    | alterDataConnectorSuffixSetUrl
    ;

alterDataConnectorSuffixProperties
@init { gParent.pushMsg("alter connector set properties statement", state); }
@after { gParent.popMsg(state); }
    : name=identifier KW_SET KW_DCPROPERTIES dcProperties
    -> ^(TOK_ALTERDATACONNECTOR_PROPERTIES $name dcProperties)
    ;

alterDataConnectorSuffixSetOwner
@init { gParent.pushMsg("alter connector set owner", state); }
@after { gParent.popMsg(state); }
    : dcName=identifier KW_SET KW_OWNER principalName
    -> ^(TOK_ALTERDATACONNECTOR_OWNER $dcName principalName)
    ;

alterDataConnectorSuffixSetUrl
@init { gParent.pushMsg("alter connector set url", state); }
@after { gParent.popMsg(state); }
    : dcName=identifier KW_SET KW_URL newUri=StringLiteral
    -> ^(TOK_ALTERDATACONNECTOR_URL $dcName $newUri)
    ;

