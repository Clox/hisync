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
	
	protected $compared=false;
	
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
		$this->compared&&trigger_error("Can't add table once compare() or sync() has been called.");
		$exceptColumns="";
		if (!empty($ignoredColumns)) {
			$ignoredColumns_impl="'".implode("','",$ignoredColumns)."'";
			$exceptColumns=PHP_EOL."AND COLUMN_NAME NOT IN ($ignoredColumns_impl)";
		}
		$columns=$this->dbs['local']->query(//get all columns of this table except manually ignored ones
			"SELECT COLUMN_NAME Field,COLUMN_KEY 'Key',EXTRA Extra,DATA_TYPE".PHP_EOL
			."FROM INFORMATION_SCHEMA.COLUMNS".PHP_EOL
			."WHERE TABLE_SCHEMA=DATABASE()".PHP_EOL
			."AND TABLE_NAME='$tableName'"
			.$exceptColumns
			)->fetchAll(PDO::FETCH_ASSOC);
		$linkedTables=$this->dbs['local']->query(//get all tables linked to by this table, and their linked columns
			"SELECT REFERENCED_TABLE_NAME,COLUMN_NAME,REFERENCED_COLUMN_NAME".PHP_EOL
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
			if ($referencedTable->mirrorField!==$referencedTable->pk)
				$table->fks[$link['COLUMN_NAME']]=$referencedTableName;
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
	
	/**
	 * 
	 * @param type $side
	 * @param SynkaTable $table
	 * @param type $ids
	 */
	public function addIdsToTranslate($side,$table,$ids) {
		if (!empty($ids)) {
			$ids=array_diff($ids,$table->syncData[$side]['pkTranslation']);
			$table->syncData[$side]['translateIds']+=
				array_fill_keys($ids, TRUE);
		}
	}
	public function compare() {
		$this->compared&&trigger_error("compare() (or sync()) has already been called, can't call again.");
		$this->compared=true;
		foreach ($this->tables as $table) {
			$this->getFieldsToSelect($table);
			$syncs=$table->syncs;
			if (key_exists('insertUnique', $syncs)) {
				$this->insertUnique($table);
			} else if (key_exists('insertCompare', $syncs)) {
				$sync=$syncs['insertCompare'];
				$this->insertCompare($table,$sync['compareField'],$sync['compareOperator']
					,$sync['subsetField']);
			}
			if (key_exists('update', $syncs)) {
				$sync=$syncs['update'];
				$this->update($table,$sync['compareField'],$sync['compareOperator']);
			}
		}
		return $this->tables;
	}
	
	public function sync() {
		$this->synced&&trigger_error("sync() has already been called, can't call again.");
		!$this->compared&&$this->compare();
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDbKey=$thisDbKey==='local'?'remote':'local';
			$otherDb=$thisDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
			foreach ($this->tables as $syncTableName=>$syncTable) {
				if (!empty($syncTable->syncData[$thisDbKey]['insertRows'])) {
					$rows=$syncTable->syncData[$thisDbKey]['insertRows'];
					$firstInsertedRowId=$this->insertRows($syncTable, $thisDbKey,$rows,$syncTable->syncData['columns'],$syncTable->updateOnDupeKey);
				}
				if (!empty($syncTable->syncData[$thisDbKey]['updateRows'])) {
					$rows=$syncTable->syncData[$thisDbKey]['updateRows'];
					$this->insertRows($syncTable, $thisDbKey,$rows,$syncTable->updateCols,true);
				} 
				
				if (!empty($syncTable->syncData[$thisDbKey]['translateInsertionIds'])) {
					foreach ($syncTable->syncData[$thisDbKey]['translateInsertionIds'] as $offset=>$oldId) {
						$syncTable->syncData[$otherDbKey]['pkTranslation'][$oldId]=$firstInsertedRowId+$offset;
						unset ($syncTable->syncData[$otherDbKey]['translateIds'][$oldId]);
					}
				}
				if (!empty($syncTable->syncData[$thisDbKey]['translateIds'])) {
					$this->translateIdViaMirror($thisDbKey,$syncTable);
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
	 * @param SynkaTable $table
	 * @param type $dbKey
	 * @param PDO $db
	 */
	protected function insertRows($table,$dbKey,$rows,$cols,$updateOnDupe) {
		$db=$this->dbs[$dbKey];
		$cols_impl="`".implode("`,`",$cols)."`";
		$colsPlaceholders='?'.str_repeat(',?', count($cols)-1);
		if (!empty($table->linkedTables)) {//if this table has any fks that need to be translated before insertion
			foreach ($table->linkedTables as $referencedTableName=>$link) {
				$columnName=$link["COLUMN_NAME"];
				$referencedTable=$this->tables[$referencedTableName];
				if ($referencedTable->mirrorField!==$link["REFERENCED_COLUMN_NAME"]) {
					$columnIndex=array_search($columnName, $cols);
					foreach ($rows as $rowIndex=>$row) {
						if ($row[$columnIndex]) {
							$rows[$rowIndex][$columnIndex]=
								$referencedTable->syncData[$dbKey]['pkTranslation'][$row[$columnIndex]];
						}
					}
				}
			}
		}
		$db->beginTransaction();
		$onDuplicate="";
		if ($updateOnDupe) {
			$onDuplicate=PHP_EOL."ON DUPLICATE KEY UPDATE".PHP_EOL;
			foreach ($cols as $columnIndex=>$column) {
				if ($columnIndex) {
					$onDuplicate.=",";
				}
				$onDuplicate.="`$column`=VALUES(`$column`)";
			}
		}
		while ($rowsPortion=array_splice($rows,0,10000)) {	
			$rowsPlaceholders="($colsPlaceholders)".str_repeat(",($colsPlaceholders)", count($rowsPortion)-1);
			$prepRowInsert=$db->prepare(
				"INSERT INTO `$table->tableName` ($cols_impl)".PHP_EOL
				."VALUES $rowsPlaceholders"
				.$onDuplicate
			);
			$values=call_user_func_array('array_merge', $rowsPortion);
			$prepRowInsert->execute($values);
		}
		$lastInsertId=$db->lastInsertId();
		$db->commit();
		return $lastInsertId;
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
	 * @param SynkaTable $table
	 * @param type $tableColumns
	 * @param type $dbKey
	 * @param type $type
	 * @param type $rows
	 */
	protected function addSyncData($table,$dbKey,$type,$rows,$updateOnDupeKey=false) {
		$cols=$type==="updateRows"?$table->updateCols:$table->syncData['columns'];
		$columnNameToInfoIndex=array_flip(array_column($table->columns, "Field"));
		foreach ($cols as $columnIndex=>$column) {
			//need to turn "1" and "0" into true and false respectively.
			//both values would otherwise always evaluate to true when doing prepared
			$columnInfo=$table->columns[$columnNameToInfoIndex[$column]];
			if ($columnInfo['DATA_TYPE']==="bit") {
				foreach ($rows as $rowIndex=>$row) {
					$rows[$rowIndex][$columnIndex]=$row[$columnIndex]==="1";
				}
			}
		}
		if ($table->idForSelect) {
			//if this table has no ids that need to be translated then we can remove the pk column from columns and
			//rows. 
			$pkIndex=array_search($table->pk, $table->syncData['columns']);
			unset($table->syncData['columns'][$pkIndex]);
			$table->syncData[$dbKey]['translateInsertionIds']=self::unsetColumn2dArray($rows,$pkIndex);
		}
			
		$table->syncData[$dbKey]['columns']=$table->syncData['columns'];//DELETE THIS?
		$table->syncData[$dbKey][$type]=$rows;
		$table->updateOnDupeKey=$updateOnDupeKey;
		
		if (!empty($table->linkedTables)) {
			foreach ($table->linkedTables as $referencedTableName=>$link) {
				$referencedTable=$this->tables[$referencedTableName];
				if ($referencedTable->mirrorField!==$link['REFERENCED_COLUMN_NAME']) {
					$linkedColumn=$link['COLUMN_NAME'];
					$fkIndex=array_search($linkedColumn, $table->syncData['columns']);
					$translateIds=array_filter(array_column($rows, $fkIndex));
					$this->addIdsToTranslate($dbKey, $referencedTable, $translateIds);
				}
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTable $table
	 * @return type
	 */
	protected function getFieldsToSelect($table) {
		$tableColumns=$table->columns;
		foreach ($tableColumns as $tableColumn) {
			if ($tableColumn['Field']!==$table->pk||$table->pk===$table->mirrorField) {
				$fields[]=$tableColumn['Field'];
			} else {
				foreach ($this->tables as $otherTableName=>$otherTable) {
					if (!empty($otherTable->fks)) {
						if ($otherTableName!=$table->tableName&&in_array($table->tableName, $otherTable->fks)) {
							$fields[]=$tableColumn['Field'];
							$table->idForSelect=true;
							break;
						}
					}
				}
			}
		}
		$table->syncData['columns']=$fields;
	}
	
	protected function implodeTableFields($fields) {
		return "`".implode('`,`',$fields).'`';
	}
}