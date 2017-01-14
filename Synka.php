<?php
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * Description of hisync
 *
 * @author oscar
 */
require_once 'SynkaTable.php';
class Synka {
	/**@var PDO[] */
	protected $dbs;//an array of the two above variables, keys being local & remote
	
	/**
	 *
	 * @var SynkaTable[] 
	 */
	protected $tables=[];
	
	protected $analyzed=false;
	
	protected $synced=false;
	
	/**
	 * 
	 * @param PDO $localDB
	 * @param PDO $remoteDB
	 */
	public function __construct($localDB,$remoteDB) {
		$this->dbs=['local'=>$localDB,'remote'=>$remoteDB];
	}
	
	/**Adds a table that should be synced or which other syncing tables are linked to.
	 * Returns a table-object with syncing-methods.
	 * @param string $tableName The name of the table
	 * @param string $mirrorField A field that uniquely can identify the rows across both sides if any.
	 *		This is used when syncing other tables that link to this table.
	 *		If no syncing tables are linking to this table or if there is no useable field then it may be omitted.
	 * @param string[] $ignoredColumns Optionally an array of columns that should be ignored for this table.
	 * @return SynkaTable Returns a table-object which has methods for syncing data in that table.*/
	public function table($tableName,$mirrorField=null,$ignoredColumns=null) {
		$this->analyzed&&trigger_error("Can't add tables once compare()(or sync()) has been called.");
		$exceptColumns="";
		if (!empty($ignoredColumns)) {
			$ignoredColumns_impl="'".implode("','",$ignoredColumns)."'";
			$exceptColumns=PHP_EOL."AND COLUMN_NAME NOT IN ($ignoredColumns_impl)";
		}
		$columns=$this->dbs['local']->query(//get all columns of this table except manually ignored ones
			"SELECT COLUMN_NAME name,COLUMN_KEY 'key',DATA_TYPE type, EXTRA extra".PHP_EOL
			."FROM INFORMATION_SCHEMA.COLUMNS".PHP_EOL
			."WHERE TABLE_SCHEMA=DATABASE()".PHP_EOL
			."AND TABLE_NAME='$tableName'"
			.$exceptColumns
			)->fetchAll(PDO::FETCH_ASSOC);
		$linkedTables=$this->dbs['local']->query(//get all tables linked to by this table, and their linked columns
			"SELECT REFERENCED_TABLE_NAME,COLUMN_NAME colName,REFERENCED_COLUMN_NAME referencedColName".PHP_EOL
			."FROM information_schema.TABLE_CONSTRAINTS i".PHP_EOL
			."LEFT JOIN information_schema.KEY_COLUMN_USAGE k"
				." ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME AND i.TABLE_SCHEMA=k.CONSTRAINT_SCHEMA".PHP_EOL
			."WHERE i.CONSTRAINT_TYPE = 'FOREIGN KEY'".PHP_EOL
			."AND k.CONSTRAINT_SCHEMA = DATABASE()".PHP_EOL
			."AND k.TABLE_NAME = '$tableName'"
			.$exceptColumns
			)->fetchAll(PDO::FETCH_GROUP|PDO::FETCH_UNIQUE|PDO::FETCH_ASSOC);
		$table=$this->tables[$tableName]=new SynkaTable($tableName,$mirrorField,$columns,$linkedTables);
		foreach ($linkedTables as $referencedTableName=>$link) {
			$referencedTable=$this->tables[$referencedTableName];
			if ($referencedTable->columns[$link['referencedColName']]->mirror) {
				$table->columns[$link['colName']]->mirror=true;
			}
		}		
		return $table;
	}
	
	/**
	 * 
	 * @param SynkaTable $table
	 */
	protected function insertUnique($table) {
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
			
			$tableFields=$table->syncData['columns'];
			$tableFields_impl=$this->implodeTableFields($tableFields);
			$thisUniqueValues=$thisDb->query("SELECT `$table->mirrorField` FROM `$table->tableName`")
				->fetchAll(PDO::FETCH_COLUMN);
			$selectMissingRowsQuery="SELECT $tableFields_impl FROM `$table->tableName`";
			if (!empty($thisUniqueValues)) {
				$thisUniqueValuesPlaceholders="?".str_repeat(",?", count($thisUniqueValues)-1);
				$selectMissingRowsQuery.=PHP_EOL."WHERE `$table->mirrorField` NOT IN ($thisUniqueValuesPlaceholders)";
			}
			$prepSelectMissingRows=$otherDb->prepare($selectMissingRowsQuery);	
			$prepSelectMissingRows->execute($thisUniqueValues);
			$thisMissingRows=$prepSelectMissingRows->fetchAll(PDO::FETCH_NUM);
			if (!empty($thisMissingRows)) {
				$this->addSyncData($table,$thisDbKey,"insertRows",$thisMissingRows);
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTable $table
	 * @param type $forSide
	 * @param type $subsetCol
	 * @param type $compCol
	 * @param type $compOp*/
	protected function fetchRowsCompareFieldWithinSubset($table,$cols,$forSide,$subsetCol,$compCol,$compOp,$newSubset) {
		$fromSide=$forSide==="local"?"remote":"local";
		$translatedExtremes=[];
		$extremes=$this->dbs[$forSide]->query($query=
			"SELECT o.`$subsetCol` sub,o.`$compCol` comp FROM `$table->tableName` o".PHP_EOL
			."LEFT JOIN `$table->tableName` b ON o.`$subsetCol` = b.`$subsetCol` "
			. "AND b.`$compCol`$compOp o.`$compCol`".PHP_EOL
			."WHERE b.`$compCol` is NULL")
			->fetchAll(PDO::FETCH_ASSOC);
		$tableFields_impl=$this->implodeTableFields($cols);
		$query="SELECT $tableFields_impl\nFROM `$table->tableName`";
		if (!empty($extremes)) {
			
			//if $subsetField either is a FK pointing to a table where the PK is not also a mirrorField
			//or if it is the PK of the current table and it is not also a mirrorField
			//then $extremes[*]['sub'] will have to be translated here
			if ($subsetCol===$table->pk&&$table->pk!==$table->mirrorField) {
				$translatePkTable=$table;
			} else {
				$linkedTableName=self::getRowByColumn($subsetCol,"COLUMN_NAME",$table->linkedTables);
				if ($linkedTableName&&$table->linkedTables[$linkedTableName]["REFERENCED_COLUMN_NAME"]
						!==$this->tables[$linkedTableName]->mirrorField) {
					$translatePkTable=$this->tables[$linkedTableName];
				}
			}
			
			if (isset($translatePkTable)) {
				$idsToTranslate=array_column($extremes,"sub");
				$this->addIdsToTranslate($fromSide,$translatePkTable,$idsToTranslate);
				$this->translateIdViaMirror($fromSide,$translatePkTable,true);
				foreach ($extremes as $extreme) {
					if (key_exists($extreme['sub'], $translatePkTable->syncData[$fromSide]['pkTranslation'])) {
						$translatedExtremes[]=$translatePkTable->syncData[$fromSide]['pkTranslation'][$extreme['sub']];
						$translatedExtremes[]=$extreme['comp'];
					}
				}
			} else {
				foreach ($extremes as $extreme) {
					$translatedExtremes[]=$extreme['sub'];
					$translatedExtremes[]=$extreme['comp'];
				}
			}
			if (!empty($translatedExtremes)) {
				$query.=" WHERE `$compCol`$compOp".PHP_EOL
					."CASE `$subsetCol`".PHP_EOL;
				$query.=str_repeat("	WHEN ? THEN ?\n", count($translatedExtremes)/2);
				$query.="	ELSE '".($compOp==="<"^$newSubset?"18446744073709551615":"-9223372036854775808")."'\nEND";
			}
		}
		$prepSelectRows=$this->dbs[$fromSide]->prepare($query);
		$prepSelectRows->execute($translatedExtremes);
		$rows=$prepSelectRows->fetchAll(PDO::FETCH_NUM);
		return $rows;
	}
	
	/**
	 * 
	 * @param SynkaTable $table
	 * @param type $compareField
	 * @param type $compareOperator
	 * @param type $updateFields
	 */
	protected function update($table,$compareField,$compareOperator) {
		foreach ($this->dbs as $currentSide=>$currentDb) {
			$updateRows=$this->fetchRowsCompareFieldWithinSubset
					($table,$table->updateCols,$currentSide,$table->pk,$compareField,$compareOperator,false);
			if (!empty($updateRows)) {
				$this->addSyncData($table,$currentSide,"updateRows",$updateRows,true);
			}
		}
	}
	
	/**Adds the pk-field of a table to a list of fields if required, unless it already is in there.
	 * It is "required" if any other tables that are being synced are linking to this table also that the PK-field is
	 * not also a mirror-field or it wont be needed because the whole reason for adding the pk to the fields is so that
	 * it can be used for translating ids when inserting rows in other tables that are linking to this.
	 * @param SynkaTable $table*/
	protected function setTableSyncsSelectFields($table) {
		$tableLinkedTo=null;
		if ($table->pk) {
			foreach ($table->syncs as $tableSync) {
				foreach ($this->tables as $otherTable) {
					if (key_exists($table->tableName, $otherTable->linkedTables)) {
						$tableLinkedTo=true;
						break;
					}
				}
				if (!in_array($table->pk, $tableSync->copyFields)) {
					$tableSync->selectFields=$tableSync->copyFields;
					$tableSync->selectFields[]=$table->pk;
				}
			}
		}
	}
	
	public function analyze($lockTables=true) {
		$this->analyzed&&trigger_error("analyze() (or commit()) has already been called, can't call again.");
		$this->analyzed=true;
		if ($lockTables) {
			$tableNames=array_keys($this->tables);
			$tableLockStatements="`".implode("` WRITE,`",$tableNames)."` WRITE";
			foreach ($this->dbs as $db) {
				$status=$db->exec("LOCK TABLES $tableLockStatements");
			}
		}
		foreach ($this->tables as $table) {
			$this->setTableSyncsSelectFields($table);
			foreach ($table->syncs as $tableSync) {
				switch ([!!$tableSync->subsetField,$tableSync->compareOperator==="!="]) {
					case [true,true]:
						$syncData=$this->analyzeTableSyncSubsetUnique($tableSync);
					break;
					case [true,false]:
						$syncData=$this->analyzeTableSyncSubsetBeyond($tableSync);
					break;
					case [false,true]:
						$syncData=$this->analyzeTableSyncGlobalUnique($tableSync);
					break;
					case [false,false]:
						$syncData=$this->analyzeTableSyncGlobalBeyond($tableSync);
				}
				$syncData&&$this->addSyncData($tableSync,$syncData);
			}
		}
		return $this->tables;
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync*/
	protected function analyzeTableSyncGlobalBeyond($tableSync) {
		$table=$tableSync->table;
		$tableFields_impl=$this->implodeTableFields($tableSync->selectFields?:$tableSync->copyFields);
		foreach ($this->dbs as $targetSide=>$targetDb) {
			$sourceSide=$targetSide==='local'?'remote':'local';
			$sourceDb=$this->dbs[$sourceSide];
			$compareVal=$targetDb->query("SELECT MAX(`$tableSync->compareField`) FROM `$table->tableName`")
			->fetchAll(PDO::FETCH_COLUMN);
			
			$selectRowsQuery="SELECT $tableFields_impl".PHP_EOL
					."FROM `$table->tableName`";
			if ($compareVal) {
				$selectRowsQuery.=PHP_EOL."WHERE `$tableSync->compareField`$tableSync->compareOperator?";
			}
			$prepSelectRows=$sourceDb->prepare($selectRowsQuery);
			$prepSelectRows->execute($compareVal);
			$selectedRows=$prepSelectRows->fetchAll(PDO::FETCH_NUM);
			if ($selectedRows) {
				return [$targetSide=>$selectedRows];
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync*/
	protected function analyzeTableSyncGlobalUnique($tableSync) {
		$syncData=[];
		$table=$tableSync->table;
		$tableFields_impl=$this->implodeTableFields($tableSync->selectFields?:$tableSync->copyFields);
		foreach ($this->dbs as $targetSide=>$targetDb) {
			$sourceSide=$targetSide==='local'?'remote':'local';
			$sourceDb=$this->dbs[$sourceSide];
			$selectRowsQuery="SELECT $tableFields_impl".PHP_EOL
				."FROM `$table->tableName`";
			$notAmong=$targetDb->query("SELECT `$tableSync->compareField` FROM `$table->tableName`")
				->fetchAll(PDO::FETCH_COLUMN);
			if ($notAmong) {
				$placeHolders='?'.str_repeat(',?', count($notAmong)-1);
				$selectRowsQuery.=PHP_EOL."WHERE `$tableSync->compareField` NOT IN ($placeHolders)";
			}
			$prepSelectRows=$sourceDb->prepare($selectRowsQuery);
			$prepSelectRows->execute($notAmong);
			$selectedRows=$prepSelectRows->fetchAll(PDO::FETCH_NUM);
			if ($selectedRows) {
				$syncData[$targetSide]=$selectedRows;
			}
		}
		return $syncData;
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync*/
	protected function analyzeTableSyncSubsetBeyond($tableSync) {
		$table=$tableSync->table;
		foreach ($this->dbs as $targetSide=>$targetDb) {
			$sourceSide=$targetSide==='local'?'remote':'local';
			$subsetsToCompVals=$targetDb->query($query=
				"SELECT o.`$tableSync->subsetField`,o.`$tableSync->compareField` FROM `$table->tableName` o".PHP_EOL
				."LEFT JOIN `$table->tableName` b ON o.`$tableSync->subsetField` = b.`$tableSync->subsetField` "
				. "AND b.`$tableSync->compareField`$tableSync->compareOperator o.`$tableSync->compareField`".PHP_EOL
				."WHERE b.`$tableSync->compareField` is NULL")
				->fetchAll(PDO::FETCH_KEY_PAIR);

			$tableFields_impl=$this->implodeTableFields($tableSync->fields);
			$query="SELECT $tableFields_impl\nFROM `$table->tableName`";
			if ($subsetsToCompVals) {
				//if $subsetField is pk or fk and not mirror then $extremes[*]['sub'] will have to be translated
				if (!$table->mirrorFields[$tableSync->subsetField]) {
					if ($tableSync->subsetField===$table->pk) {
						$translatePkTable=$table;
					} else {
						$linkedTableName=self::getRowByColumn($tableSync->subsetField,"colName",$table->linkedTables);
						if ($linkedTableName) {
							$translatePkTable=$this->tables[$linkedTableName];
						}
					}
				}
				if (isset($translatePkTable)) {
					$idsToTranslate=array_keys($subsetsToCompVals);
					$this->addIdsToTranslate($sourceSide,$translatePkTable,$idsToTranslate);
					$this->translateIdViaMirror($sourceSide,$translatePkTable,true);
					foreach ($subsetsToCompVals as $subset=>$compVal) {
						if (key_exists($subset, $translatePkTable->translateIdFrom[$sourceSide])) {
							$subsetsAndCompVals_flat[]=$translatePkTable->translateIdFrom[$sourceSide][$subset];
							$subsetsAndCompVals_flat[]=$compVal;
						}
					}
				} else {
					foreach ($subsetsToCompVals as $subset=>$compVal) {
						$subsetsAndCompVals_flat[]=$subset;
						$subsetsAndCompVals_flat[]=$compVal;
					}
				}
				if (!empty($subsetsAndCompVals_flat)) {
					$query.=" WHERE `$tableSync->compareField`$tableSync->compareOperator".PHP_EOL
						."CASE `$tableSync->subsetField`".PHP_EOL;
					$query.=str_repeat("	WHEN ? THEN ?\n", count($subsetsAndCompVals_flat)/2)."\nEND";
					//$query.="	ELSE '".($compOp==="<"?"18446744073709551615":"-9223372036854775808")."'\nEND";
				}
			}
			$prepSelectRows=$this->dbs[$sourceSide]->prepare($query);
			$prepSelectRows->execute($subsetsAndCompVals_flat);
			$rows[$targetSide]=$prepSelectRows->fetchAll(PDO::FETCH_NUM);
		}
		return $rows;
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync*/
	protected function analyzeTableSyncSubsetUnique($tableSync) {
		trigger_error("Not yet implemented");
	}
	
	/**
	 * 
	 * @param SynkaTable $table
	 * @param type $compareField
	 * @param type $compareOperator
	 * @param type $subsetField
	 * @param type $updateOnDupeKey*/
	protected function insertCompare($table,$compareField,$compareOperator,$subsetField) {
		$cols=$table->syncData['columns'];
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDbKey=$thisDbKey==='local'?'remote':'local';
			$otherDb=$this->dbs[$otherDbKey];
			$table->syncData['columns'];
			$tableFields_impl=$this->implodeTableFields($cols);
			if (!$subsetField) {
				$extreme=$thisDb->query("SELECT MAX(`$compareField`) FROM `$table->tableName`")
					->fetch(PDO::FETCH_COLUMN);
				$query="SELECT $tableFields_impl".PHP_EOL
					."FROM `$table->tableName`";
				if ($extreme) {
					$query.=PHP_EOL."WHERE `$compareField`$compareOperator?";
				}
				$prepSelectMissingRows=$otherDb->prepare($query);
				$prepSelectMissingRows->execute([$extreme]);
				$thisMissingRows=$prepSelectMissingRows->fetchAll(PDO::FETCH_NUM);
			} else {
				$thisMissingRows=$this->fetchRowsCompareFieldWithinSubset
						($table,$cols,$thisDbKey,$subsetField,$compareField,$compareOperator,true);
			}
			if (!empty($thisMissingRows)) {
				$this->addSyncData($table,$thisDbKey,"insertRows",$thisMissingRows);
				if (!$subsetField) {
					break;
				}
			}
		}
	}
	
	public function sync() {
		$this->synced&&trigger_error("sync() has already been called, can't call again.");
		!$this->analyzed&&$this->compare();
		foreach ($this->dbs as $targetSide=>$targetDb) {
			$sourceSide=$targetSide==='local'?'remote':'local';
			$sourceDb=$targetDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
			foreach ($this->tables as $tableName=>$table) {
				foreach ($table->syncs as $tableSync) {
					if ($tableSync->syncData) {
						$rows=$tableSync->syncData[$targetSide];
						if ($rows) {
							$this->copyData($tableSync,$targetSide);
						}
					}
				}
				if (!empty($table->syncData[$targetSide]['updateRows'])) {
					$rows=$table->syncData[$targetSide]['updateRows'];
					$this->copyData($table, $targetSide,$rows,$table->updateCols,true);
				} 
				
				if (!empty($table->syncData[$targetSide]['translateInsertionIds'])) {
					foreach ($table->syncData[$targetSide]['translateInsertionIds'] as $offset=>$oldId) {
						$table->syncData[$sourceSide]['pkTranslation'][$oldId]=$firstInsertedRowId+$offset;
						unset ($table->syncData[$sourceSide]['translateIds'][$oldId]);
					}
				}
				if (!empty($table->syncData[$targetSide]['translateIds'])) {
					$this->translateIdViaMirror($targetSide,$table);
				}
			}
		}
	}
	
	/**
	 * 
	 * @param PDO $fromDb
	 * @param PDO $toDb
	 * @param SynkaTable $table
	 * @param type $fromIds
	 */
	protected function translateIdViaMirror($fromSide,$table,$possibleUnsyncedRows=false) {
		$fromIds=array_keys($table->syncData[$fromSide]['translateIds']);
		$toSide=$fromSide==='local'?'remote':'local';
		$table->syncData[$fromSide]['translateIds']=[];
		sort ($fromIds);
		$placeholders='?'.str_repeat(',?', count($fromIds)-1);
		$prepSelectMirrorValues=$this->dbs[$toSide]->prepare($query=
			"SELECT `$table->mirrorField` FROM `$table->tableName`".PHP_EOL
			."WHERE `$table->pk` IN ($placeholders) ORDER BY `$table->pk`");
		$prepSelectMirrorValues->execute($fromIds);
		$mirrorValues=$prepSelectMirrorValues->fetchAll(PDO::FETCH_COLUMN);
		$result=[];
		if (!$possibleUnsyncedRows) {
			$oldPkToMirror=array_combine($fromIds,$mirrorValues);
			asort($oldPkToMirror);
			$prepSelectNewIds=$this->dbs[$fromSide]->prepare(
				"SELECT `$table->pk` FROM `$table->tableName`".PHP_EOL
				."WHERE `$table->mirrorField` IN ($placeholders) ORDER BY `$table->mirrorField`");
			$prepSelectNewIds->execute($mirrorValues);
			$newIds=$prepSelectNewIds->fetchAll(PDO::FETCH_COLUMN);
			$result=array_combine(array_keys($oldPkToMirror), $newIds);
		} else {
			$mirrorToOldPk=array_combine($mirrorValues,$fromIds);
			$prepSelectNewIds=$this->dbs[$fromSide]->prepare(
				"SELECT `$table->pk`,`$table->mirrorField` FROM `$table->tableName`".PHP_EOL
				."WHERE `$table->mirrorField` IN ($placeholders) ORDER BY `$table->mirrorField`");
			$prepSelectNewIds->execute($mirrorValues);
			$newIdToMirror=$prepSelectNewIds->fetchAll(PDO::FETCH_KEY_PAIR);
			foreach ($newIdToMirror as $newId=>$mirror) {
				$result[$mirrorToOldPk[$mirror]]=$newId;
			}
		}
		return $table->syncData[$fromSide]['pkTranslation']+=$result;
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync
	 * @param string $side
	 */
	protected function translateFks ($tableSync,$side) {
		$sourceSide=$side==="local"?"remote":"local";
		$table=$tableSync->table;
		if (!empty($table->linkedTables)) {//if this table has any fks that need to be translated before insertion
			foreach ($table->linkedTables as $referencedTableName=>$link) {
				$columnName=$link["COLUMN_NAME"];
				$referencedTable=$this->tables[$referencedTableName];
				if (!$table->columns[$link["REFERENCED_COLUMN_NAME"]]->mirror) {
					$columnIndex=array_search($columnName, $tableSync->copyFields);
					foreach ($tableSync->syncData[$side] as $rowIndex=>$row) {
						if ($row[$columnIndex]) {
							$tableSync->syncData[$side][$rowIndex][$columnIndex]=
								$referencedTable->translateIdFrom[$sourceSide][$row[$columnIndex]];
						}
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync
	 * @param string $side
	 * @return type*/
	protected function copyData($tableSync,$side) {
		//insert using temp table:
		//https://ricochen.wordpress.com/2011/06/21/bulk-update-in-mysql-with-the-use-of-temporary-table/
		$table=$tableSync->table;
		$this->translateFks($tableSync, $side);
		
		//choose the best suited strategy for inserting/updating
		//1: if we are copying neither pk nor unique fields then we may do a simple insert		
		//2: else if the table has no auto-incremented PK then we may do insert on duplicate update even with pk or unique
		//3: else lock table, check which pk/uniques are occupied. insert those that weren't, unlock and update the rest
		
		$copyingUnique=$hasAutoPk=null;
		foreach ($tableSync->copyFields as $colName) {
			$colInfo=$table->columns[$colName];
			if ($colInfo->type==="UNI"||$colInfo->type==="PRI") {
				$copyingUnique=true;
				if ($table->pk&&$table->columns[$table->pk]->extra==="auto_increment") {
					$hasAutoPk=true;
				}
			}
		}
		
		if (!$copyingUnique||!$hasAutoPk) {
			$this->insertRows($tableSync, $side, $copyingUnique);
		} else {
			$this->insertAndUpdateRowsWithLock($tableSync,$side,$colName);
		}
	}
	
	
	protected function insertRows($tableSync,$side,$onDupeIgnore) {
		$cols=$tableSync->copyFields;
		$cols_impl=$this->implodeTableFields($cols);
		$colsPlaceholders='?'.str_repeat(',?', count($cols)-1);
		$rows=$tableSync->syncData[$side];
		$table=$tableSync->table;
		$sourceSide=$side==="local"?"remote":"local";
		$suffix="";
		if ($onDupeIgnore) {
			$suffix=PHP_EOL."ON DUPLICATE KEY UPDATE".PHP_EOL;
			foreach ($tableSync->copyFields as $columnIndex=>$columnName) {
				if ($columnIndex) {
					$suffix.=",";
				}
				$suffix.="`$columnName`=VALUES(`$columnName`)";
			}
		}
		$rowIndex=0;
		while ($rowsPortion=array_splice($rows,0,10000)) {
			$rowsPlaceholders="($colsPlaceholders)".str_repeat(",($colsPlaceholders)", count($rowsPortion)-1);
			$prepRowInsert=$db->prepare(
				"INSERT INTO `$table->tableName` ($cols_impl)".PHP_EOL
				."VALUES $rowsPlaceholders"
				.$suffix
			);
			$values=call_user_func_array('array_merge', $rowsPortion);
			$prepRowInsert->execute($values);
			if ($tableSync->selectFields) {//if other tables link to this one and need the generated ids for translating
				$firstInsertedId=$db->lastInsertId();//actually the first generated id from the last statement
				foreach ($tableSync->insertionIds[$side] as $sourceId) {
					$table->translateIdFrom[$sourceSide][$sourceId]=$firstInsertedId+$rowIndex++;
				}
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync
	 * @param string $side
	 */
	protected function insertAndUpdateRowsWithLock($tableSync,$side,$uniqueField) {
		$db=$this->dbs[$side];
		$table=$tableSync->table;
		$cols=$tableSync->copyFields;
		$uniqueIndex=array_search($uniqueField,$tableSync->copyFields);
		$uniqueValues=array_column($tableSync->syncData[$side], $uniqueIndex);
		
		$uniquePlaceholders='?'.str_repeat('?,', count($uniqueValues));
		$db->prepare("SELECT `$uniqueField` FROM `$table->tableName WHERE `$uniqueField` IN ($uniquePlaceholders)");
		$db->exec("LOCK TABLES `{$table->tableName}");
		$db->exec($uniqueValues);
		$confirmedUniques=$db->fetchAll(PDO::FETCH_UNIQUE);
		$updates=[];
		foreach ($tableSync->syncData[$side] as $rowIndex=>$row) {
			$unique=$row[$uniqueIndex];
			if (key_exists($unique,$confirmedUniques)) {
				unset($row[$uniqueIndex]);
				$updates[$unique]=array_values($row);
				unset ($tableSync->syncData[$side][$rowIndex]);
			}
		}
		if ($tableSync->syncData[$side]) {
			$this->insertRows($tableSync,$side,false);
		}
		$db -> exec('UNLOCK TABLES;');
		if ($updates) {
			$this->updateRows($tableSync,$side,$updates,$uniqueField);
		}
	}
	
	protected function updateRows($tableSync,$side,$updates,$uniqueField) {
		
	}


	static function unsetColumn2dArray(&$array,$index) {
		foreach ($array as $rowIndex=>$row) {
			$values[]=$array[$rowIndex][$index];
			unset ($array[$rowIndex][$index]);
		}
		return $values;
	}
	
	static function getRowByColumn($needle,$column,$haystack) {
		foreach ($haystack as $rowIndex=>$row) {
			if (key_exists($column, $row)&&$row[$column]===$needle) {
				return $rowIndex;
			}
		}
		return false;
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync
	 * @param array $syncData*/
	protected function addSyncData($tableSync,$syncData) {
		$table=$tableSync->table;
		foreach ($syncData as $side=>$rows) {
			foreach ($tableSync->copyFields as $columnIndex=>$columnName) {
				//need to turn "1" and "0" into true and false respectively.
				//both values would otherwise always evaluate to true when doing prepared
				if ($table->columns[$columnName]->type==="bit") {
					foreach ($rows as $rowIndex=>$row) {
						$syncData[$side][$rowIndex][$columnIndex]=$row[$columnIndex]==="1";
					}
				}
			}
			if ($tableSync->selectFields) {
				$pkIndex=array_search($table->pk, $tableSync->selectFields);
				$tableSync->insertionIds[$side]=self::unsetColumn2dArray($rows,$pkIndex);
			}
			
			//if this table links to another table, get the ids of the linked rows, and add them to be translated
			if (!empty($table->linkedTables)) {
				foreach ($table->linkedTables as $referencedTableName=>$link) {
					$linkedColumnName=$link['colName'];
					if (!$table->columns[$linkedColumnName]->mirror) {
						$referencedTable=$this->tables[$referencedTableName];
						$fkIndex=array_search($linkedColumnName, $tableSync->copyFields);
						$translateIds=array_filter(array_column($syncData[$side], $fkIndex));
						$referencedTable->addIdsToTranslate($side,$translateIds);
					}
				}
			}
		}
		$tableSync->syncData=$syncData;
	}
	
	protected function implodeTableFields($fields) {
		return "`".implode('`,`',$fields).'`';
	}
}