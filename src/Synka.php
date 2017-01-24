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
	
	protected $commited=false;
	
	protected $tablesLocked=false;
	
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
			$table->columns[$link['colName']]->fk=$referencedTableName;
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
	
	/**Adds the pk-field of a table to a list of fields if required, unless it already is in there.
	 * The use of it is to be able to fetch and save the pk's of the source db even if they wont be inserted
	 * intro the target db, then when the data is inserted into the target db, we can use the list to figure out
	 * what the id of each row is in the source-db without querying for it, so that we can use it for id-translation.
	 * The pk-field is added ifany other tables that are being synced are linking to this table and also that the
	 * PK-field is not also a mirror-field since then it wont be needed.
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
		if ($this->tablesLocked=$lockTables) {
			$lockStmt="";
			foreach ($this->tables as $tableName=>$table) {
				if ($table->syncs) {
					if ($lockStmt) {
						$lockStmt.=',';
					}
					$lockStmt.="`$tableName` WRITE";
					foreach ($table->syncs as $tableSync) {
						if ($tableSync->subsetFields&&$tableSync->compareOperator!=="!=") {
							$lockStmt.=",`$tableName` `{$tableName}2` READ";
						}
					}
				}
			}
			foreach ($this->dbs as $db) {
				$db->exec("LOCK TABLES $lockStmt");
			}
		}
		foreach ($this->tables as $table) {
			$this->setTableSyncsSelectFields($table);
			foreach ($table->syncs as $tableSync) {
				switch ([!!$tableSync->subsetFields,$tableSync->compareOperator==="!="]) {
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
			$compareVal=$targetDb->query("SELECT MAX(`{$tableSync->compareFields[0]}`) FROM `$table->tableName`")
			->fetchAll(PDO::FETCH_COLUMN);
			
			$selectRowsQuery="SELECT $tableFields_impl".PHP_EOL
					."FROM `$table->tableName`";
			if ($compareVal) {
				$selectRowsQuery.=PHP_EOL."WHERE `{$tableSync->compareFields[0]}`$tableSync->compareOperator?";
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
			$selectRowsQuery="SELECT $tableFields_impl".PHP_EOL
				."FROM `$table->tableName`";
			$notAmong=$this->getGlobalUniqueCompareValues($targetSide,$tableSync);
			$this->translateFields($tableSync, $targetSide, false, $notAmong, $tableSync->compareFields);
			if ($notAmong) {
				$uniqueFields_impl=$this->implodeTableFields($tableSync->compareFields);
				if (count($tableSync->compareFields)===1) {
					$placeHolders='?'.str_repeat(',?', count($notAmong)-1);
				} else {
					$fieldPlaceholders='?'.str_repeat(',?', count($tableSync->compareFields)-1);
					$placeHolders="($fieldPlaceholders)".str_repeat(",($fieldPlaceholders)", count($notAmong)-1);
				}
				$selectRowsQuery.=PHP_EOL."WHERE ($uniqueFields_impl) NOT IN ($placeHolders)";
				$notAmong=call_user_func_array('array_merge', $notAmong);
			}
			$prepSelectRows=$this->dbs[$sourceSide]->prepare($selectRowsQuery);
			$prepSelectRows->execute($notAmong);
			if ($selectedRows=$prepSelectRows->fetchAll(PDO::FETCH_NUM))
				$syncData[$targetSide]=$selectedRows;
		}
		return $syncData;
	}
	
	protected function getGlobalUniqueCompareValues($side,$tableSync) {
		$selectFields_impl=$this->implodeTableFields($tableSync->compareFields);
		$numUniqueFields=count($tableSync->compareFields);
		$alsoGetPk=null;
		foreach ($tableSync->table->syncs as $otherTableSync) {//should we just be checking the current $tableSync...?
			if ($otherTableSync->copyStrategy===3) {
				$alsoGetPk=true;
				$selectFields_impl.=",`{$tableSync->table->pk}`";
				break;
			}
		}
		$uniqueRows=$this->dbs[$side]->query("SELECT $selectFields_impl FROM `{$tableSync->table->tableName}`")
			->fetchAll(PDO::FETCH_NUM);
		if ($uniqueRows) {
			if ($numUniqueFields===1) {
				if ($alsoGetPk) {
					$tableSync->uniqueMap=array_combine(array_column($uniqueRows,0),array_column($uniqueRows,1));
				} else {
					$tableSync->uniqueMap=array_fill_keys(array_column($uniqueRows,0),true);
				}
			} else {
				foreach ($uniqueRows as $uniqueRow) {
					$tableSync->uniqueMap[implode(',',array_slice($uniqueRow,0,$numUniqueFields))]=
							$alsoGetPk?$uniqueRow[$numUniqueFields]:true;
				}
			}
			if ($alsoGetPk) {
				self::unsetColumn2dArray($uniqueRows, $numUniqueFields);
			}
		}
		return $uniqueRows;
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync*/
	protected function analyzeTableSyncSubsetBeyond($tableSync) {
		$result=[];
		$compValsJoins=$compValsSelectSubsets=$compValsQuery="";
		$tableName=$tableSync->table->tableName;
		$compareField=$tableSync->compareFields[0];
		foreach ($tableSync->subsetFields as $subsetField) {
			$compValsJoins.="$tableName.`$subsetField`={$tableName}2.`$subsetField` AND ";
			$compValsSelectSubsets.="`$tableName`.`$subsetField`,";
		}
		$compValsQuery="SELECT {$compValsSelectSubsets}`$tableName`.`$compareField` FROM `$tableName`".PHP_EOL
				."LEFT JOIN `$tableName` {$tableName}2 ON $compValsJoins"
				."{$tableName}2.`$compareField`$tableSync->compareOperator `$tableName`.`$compareField`".PHP_EOL
				."WHERE {$tableName}2.`$compareField` is NULL";
		foreach ($this->dbs as $targetSide=>$targetDb) {
			$sourceSide=$targetSide==='local'?'remote':'local';
			$compVals=$targetDb->query($compValsQuery)->fetchAll(PDO::FETCH_NUM);
			if ($compVals) {
				//if any of the subsetFields are pk/fk and not mirror then they will have to be translated in $compVals
				$this->translateFields($tableSync,$targetSide,false,$compVals,$tableSync->subsetFields);
				if ($compVals) {
					$tableFields_impl=$this->implodeTableFields($tableSync->selectFields?:$tableSync->copyFields);
					$rowsQuery="SELECT $tableFields_impl\nFROM `$tableName`".PHP_EOL
						."WHERE `$compareField`{$tableSync->compareOperator}CASE".PHP_EOL;
						if (count($tableSync->subsetFields)===1) {
							$rowsQuery.=" `{$tableSync->subsetFields[0]}`".PHP_EOL;
							$subsetCase="WHEN ? THEN ?".PHP_EOL;
						} else {
							$subsetCase="WHEN (".$this->implodeTableFields($tableSync->subsetFields).")"
								."=(?".str_repeat(",?",count($tableSync->subsetFields)-1).") THEN ?".PHP_EOL;
						}
					$rowsQuery.=str_repeat($subsetCase, count($compVals))."END";
					$prepSelectRows=$this->dbs[$sourceSide]->prepare($rowsQuery);
					$prepSelectRows->execute(call_user_func_array('array_merge', $compVals));
					($rows=$prepSelectRows->fetchAll(PDO::FETCH_NUM))&&$result[$targetSide]=$rows;
				}
			}
		}
		return $result;
	}
	
	/**Takes a 2D array and a specification of fields/columns and translates IDs.
	 * Non-ID fields will be ignored. If there's an ID that can't be translated because its counterpart isn't present
	 * in the other DB then its row will be deleted.
	 * @param SynkaTableSync $table Only used for looking up info on the fields
	 * @param string "local"|"remote" $fromSide The side which the IDs should be translated from
	 * @param array[] $data The data with columns to be translated
	 * @param string[] $fields field-names which should have the same index as its column in $data.
	 * Nulls will result in those columns being ignored. Fields which arent translateable will be ignored too.
	 * @$possibleUnknownIds If this is true then it means there is possibly IDs that don't yet have a counterpart
	 * on the from-side*/
	protected function translateFields($tableSync,$fromSide,$insertState=true,&$data=null,$fields=null) {
		if ($insertState) {
			$data=&$tableSync->syncData[$fromSide==='local'?'remote':'local'];
			$fields=$tableSync->copyFields;
		}
		if ($data) {
			foreach ($fields as $fieldIndex=>$fieldName) {
				if (!$tableSync->table->columns[$fieldName]->mirror) {
					$translateTable=null;
					if ($tableSync->table->columns[$fieldName]->fk){
						$translateTable=$this->tables[$tableSync->table->columns[$fieldName]->fk];
					} else if ($fieldName===$tableSync->table->pk) {
						//do we ever actually need to translate pks with this function?
						//either we are inserting data an if it is mirror pk then translation is not needed(and this
						//block wont even be reached), if its not mirror then the pk shouldn't be copied, 
						//should it...? Or am I not thinking correctly now?
						$translateTable=$tableSync->table;
					}
					if ($translateTable) {
						if (!$insertState) {
							$translateTable->addIdsToTranslate($fromSide, array_column($data,$fieldIndex));
							$this->translateIdViaMirror($fromSide,$translateTable,true);
						}
						foreach ($data as $rowIndex=>$row) {
							$id=$row[$fieldIndex];
							if ($id) {//because it may be a null fk
								if ($insertState||key_exists($id,$translateTable->translateIdFrom[$fromSide])) {
									$data[$rowIndex][$fieldIndex]=$translateTable->translateIdFrom[$fromSide][$id];
								} else {
									unset($data[$rowIndex]);
								}
							}
						}
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTableSync $tableSync*/
	protected function analyzeTableSyncSubsetUnique($tableSync) {
		trigger_error("Not yet implemented");
	}
	
	public function commit() {
		$this->commited&&trigger_error("commit() has already been called, can't call again.");
		!$this->analyzed&&$this->analyze();
		foreach ($this->tables as $table) {
			foreach ($this->dbs as $targetSide=>$targetDb) {
			$sourceSide=$targetSide==='local'?'remote':'local';
				foreach ($table->syncs as $tableSync) {		
					if (isset($tableSync->syncData[$targetSide])) {
						$this->translateFields($tableSync, $sourceSide);
						if ($tableSync->copyStrategy<3) {
							$this->insertRows($tableSync, $targetSide, $tableSync->copyStrategy===2);
						} else {
							$this->insertAndUpdateRows($tableSync, $targetSide);
						}
					}
				}
				$this->translateIdViaMirror($sourceSide,$table);
			}
		}
		$this->tablesLocked&&$targetDb->exec('UNLOCK TABLES;');
	}
	
	/**
	 * 
	 * @param PDO $fromDb
	 * @param PDO $toDb
	 * @param SynkaTable $table
	 * @param type $fromIds
	 */
	protected function translateIdViaMirror($fromSide,$table,$possibleUnsyncedRows=false) {
		$fromIds=array_keys($table->translateIdFrom[$fromSide],NULL);
		if ($fromIds) {
			$toSide=$fromSide==='local'?'remote':'local';
			sort ($fromIds);
			$placeholders='?'.str_repeat(',?', count($fromIds)-1);
			$prepSelectMirrorValues=$this->dbs[$fromSide]->prepare($query=
				"SELECT `$table->mirrorField` FROM `$table->tableName`".PHP_EOL
				."WHERE `$table->pk` IN ($placeholders) ORDER BY `$table->pk`");
			$prepSelectMirrorValues->execute($fromIds);
			$mirrorValues=$prepSelectMirrorValues->fetchAll(PDO::FETCH_COLUMN);
			$result=[];
			if (!$possibleUnsyncedRows) {
				$oldIdToMirror=array_combine($fromIds,$mirrorValues);
				asort($oldIdToMirror);
				$prepSelectNewIds=$this->dbs[$toSide]->prepare(
					"SELECT `$table->pk` FROM `$table->tableName`".PHP_EOL
					."WHERE `$table->mirrorField` IN ($placeholders) ORDER BY `$table->mirrorField`");
				$prepSelectNewIds->execute($mirrorValues);
				$newIds=$prepSelectNewIds->fetchAll(PDO::FETCH_COLUMN);
				$result=array_combine(array_keys($oldIdToMirror), $newIds);
				$table->translateIdFrom[$fromSide]=$result+$table->translateIdFrom[$fromSide];
			} else {
				$mirrorToOldId=array_combine($mirrorValues,$fromIds);
				$prepSelectNewIds=$this->dbs[$toSide]->prepare(
					"SELECT `$table->mirrorField`,`$table->pk` FROM `$table->tableName`".PHP_EOL
					."WHERE `$table->mirrorField` IN ($placeholders) ORDER BY `$table->mirrorField`");
				$prepSelectNewIds->execute($mirrorValues);
				$mirrorToNewId=$prepSelectNewIds->fetchAll(PDO::FETCH_KEY_PAIR);
				foreach ($mirrorToOldId as $mirror=>$oldId) {
					if (key_exists($mirror, $mirrorToNewId)) {
						$table->translateIdFrom[$fromSide][$oldId]=$mirrorToNewId[$mirror];
					} else {
						unset ($table->translateIdFrom[$fromSide][$oldId]);
					}
				}
			}
		}
	}	
	
	protected function insertRows($tableSync,$side,$onDupeUpdate) {
		$cols=$tableSync->copyFields;
		$cols_impl=$this->implodeTableFields($cols);
		$colsPlaceholders='?'.str_repeat(',?', count($cols)-1);
		$rows=$tableSync->syncData[$side];
		$table=$tableSync->table;
		$sourceSide=$side==="local"?"remote":"local";
		$suffix="";
		if ($onDupeUpdate) {
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
			$prepRowInsert=$this->dbs[$side]->prepare(
				"INSERT INTO `$table->tableName` ($cols_impl)".PHP_EOL
				."VALUES $rowsPlaceholders"
				.$suffix
			);
			$values=call_user_func_array('array_merge', $rowsPortion);
			$prepRowInsert->execute($values);
			if ($tableSync->selectFields) {//if other tables link to this one and need the generated ids for translating
				$firstInsertedId=$this->dbs[$side]->lastInsertId();//actually first generated id from the last statement
				foreach ($tableSync->insertsSourceIds[$side] as $sourceId) {
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
	protected function insertAndUpdateRows($tableSync,$side) {
		trigger_error("Not implemented.");
		//makeanalyzeTableSyncGlobalUnique if run save the uniques in the tableSync-object or something so this function
		//can now which uniques are new and which are old without having to do an extra query.
		//if it hasn't been run then fetch the uniques now and figure out new and old
		$db=$this->dbs[$side];
		$table=$tableSync->table;
		$cols=$tableSync->copyFields;
		$compareFields=$tableSync->compareFields;
		foreach ($tableSync->table->syncs as $otherTableSync) {
//			if ($compareFields===$otherTableSync->subsetFields)
		}
		if (count($tableSync->compareFields)===1) {
			$uniqueField=$tableSync->compareFields[0];
			$newUniqueIndex=array_search($uniqueField,$tableSync->copyFields);
			$newUniqueValues=array_column($tableSync->syncData[$side], $newUniqueIndex);
			$oldUniques=$table->columns[$uniqueField]->allUniques[$side];
			if (!$oldUniques) {
				$uniquePlaceholders='?'.str_repeat(',?', count($newUniqueValues)-1);
				$prepGetOldUniques=$db->prepare
					("SELECT `$uniqueField` FROM `$table->tableName` WHERE `$uniqueField` IN ($uniquePlaceholders)");
				$prepGetOldUniques->execute($newUniqueValues);
				$oldUniques=array_fill_keys($prepGetOldUniques->fetchAll(PDO::FETCH_COLUMN),1);
			}
		}
		foreach ($tableSync->compareFields as $uniqueField) {
			$uniquFields[array_search($uniqueField, $tableSync->copyFields)]=$uniqueField;
		}
		
		
		
		
		
		
		$updates=[];
		foreach ($tableSync->syncData[$side] as $rowIndex=>$row) {
			$unique=$row[$newUniqueIndex];
			if (key_exists($unique,$oldUniques)) {
				unset($row[$newUniqueIndex]);
				$updates[$unique]=array_values($row);
				unset ($tableSync->syncData[$side][$rowIndex]);
			}
		}
		if ($tableSync->syncData[$side]) {
			$this->insertRows($tableSync,$side,false);
		}
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
			$sourceSide=$side==='local'?'remote':'local';
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
				$tableSync->insertsSourceIds[$side]=self::unsetColumn2dArray($syncData[$side],$pkIndex);
			}
			
			//if this table links to another table, get the ids of the linked rows, and add them to be translated
			if (!empty($table->linkedTables)) {
				foreach ($table->linkedTables as $referencedTableName=>$link) {
					$fkIndex=array_search($link['colName'], $tableSync->copyFields);
					if ($fkIndex!==FALSE&&!$table->columns[$link['colName']]->mirror) {
						$referencedTable=$this->tables[$referencedTableName];
						$translateIds=array_filter(array_column($syncData[$side], $fkIndex));
						$referencedTable->addIdsToTranslate($sourceSide,$translateIds);
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