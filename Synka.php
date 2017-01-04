<?php
//TODO
//Get rid of $linkedTables in SynkaTable and only use syncingData instead
//Get rid of mirrorPK? instead only have pkField and mirror Field and check for equality
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
	
	protected $syncData=[];
	
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
			"SELECT COLUMN_NAME Field,COLUMN_KEY 'Key',EXTRA Extra".PHP_EOL
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
		$table=$this->tables[]=new SynkaTable($tableName,$mirrorField,$columns,$linkedTables);
		$this->syncData[$tableName]=['name'=>$tableName,'mirrorField'=>$mirrorField,'pkField'=>$table->pk,
			'local'=>['translateIds'=>[]],'remote'=>['translateIds'=>[]]];
		foreach ($linkedTables as $referencedTable=>$link) {
			if ($this->syncData[$referencedTable]['mirrorField']!==$this->syncData[$referencedTable]['pkField'])
				$this->syncData[$tableName]['fks'][$link['COLUMN_NAME']]=$referencedTable;
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
			
			$tableFields=$this->getFieldsToSelect($table,$thisDbKey);
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
				$this->addSyncData($table,$tableFields,$thisDbKey,"insertRows",$thisMissingRows);
			}
		}
	}
	
	protected function insertCompare($table,$compareField,$compareOperator) {
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
			$tableFields=$this->getFieldsToSelect($table,$thisDbKey);
			$tableFields_impl=$this->implodeTableFields($tableFields);
			$thisExtremeValue=$thisDb->query("SELECT MAX(`$compareField`) FROM `$table->tableName`")
					->fetch(PDO::FETCH_COLUMN);
			$query="SELECT $tableFields_impl".PHP_EOL
				."FROM `$table->tableName`";
			if ($thisExtremeValue) {
				$query.=PHP_EOL."WHERE `$compareField`$compareOperator?";
			}
			$prepSelectMissingRows=$otherDb->prepare($query);
			$prepSelectMissingRows->execute([$thisExtremeValue]);
			$thisMissingRows=$prepSelectMissingRows->fetchAll(PDO::FETCH_NUM);
			if (!empty($thisMissingRows)) {
				$this->addSyncData($table,$tableFields,$thisDbKey,"insertRows",$thisMissingRows);
				break;
			}
		}
	}
	
	public function compare() {
		$this->compared&&trigger_error("compare() (or sync()) has already been called, can't call again.");
		$this->compared=true;
		foreach ($this->tables as $table) {
			$syncs=$table->syncs;
			if (key_exists('insertUnique', $syncs)) {
				$this->insertUnique($table);
			} else if (key_exists('insertCompare', $syncs)) {
				$sync=$syncs['insertCompare'];
				$this->insertCompare($table,$sync['compareField'],$sync['compareOperator']);
			}
		}
		return $this->syncData;
	}
	
	public function sync() {
		$this->synced&&trigger_error("sync() has already been called, can't call again.");
		!$this->compared&&$this->compare();
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
			foreach ($this->syncData as $syncTableName=>$syncTable) {
				if (isset($syncTable[$thisDbKey]['insertRows'])) {
					$firstInsertedRowId=$this->insertRows($syncTable, $thisDbKey,$thisDb);
				}
				if (!empty($syncTable[$thisDbKey]['translateIds'])) {
					$pkTranslation=[];
					$translationsToProcess=$syncTable[$thisDbKey]['translateIds'];
					if (!empty($syncTable[$thisDbKey]['translateInsertionIds'])) {
						foreach ($syncTable[$thisDbKey]['translateInsertionIds'] as $offset=>$oldId) {
							$pkTranslation[$oldId]=$firstInsertedRowId+$offset;
						}
						$translationsToProcess=array_keys(array_diff_key($translationsToProcess,$pkTranslation));
					}

					if (!empty($translationsToProcess)) {
						$pkTranslation+=$this->translatePkViaMirror
							($otherDb,$thisDb,$syncTable,array_keys($translationsToProcess));
					}
					$this->syncData[$syncTableName][$thisDbKey]['pkTranslation']=$pkTranslation;
					//translateIds is an associative array of all ids we need translated as keys, and simply
					//true as the values
					//translateInsertionIds is an indexed array where index is the offset in the rows of the just
					//inserted rows, and value is id from the table they were copied from
				}
			}
		}
	}
	
	/**
	 * 
	 * @param PDO $fromDb
	 * @param PDO $toDb
	 * @param type $syncTable
	 * @param type $fromIds
	 */
	protected function translatePkViaMirror($fromDb, $toDb, $syncTable, $fromIds) {
		sort ($fromIds);
		$placeholders='?'.str_repeat(',?', count($fromIds)-1);
		$prepSelectMirrorValues=$fromDb->prepare($query=
			"SELECT `$syncTable[mirrorField]` FROM `$syncTable[name]`".PHP_EOL
			."WHERE `$syncTable[pkField]` IN ($placeholders) ORDER BY `$syncTable[pkField]`");
		$prepSelectMirrorValues->execute($fromIds);
		$mirrorValues=$prepSelectMirrorValues->fetchAll(PDO::FETCH_COLUMN);
		$oldPkToMirror=array_combine($fromIds,$mirrorValues);
		asort($oldPkToMirror);
		$prepSelectNewIds=$toDb->prepare(
			"SELECT `$syncTable[pkField]` FROM `$syncTable[name]`".PHP_EOL
			."WHERE `$syncTable[mirrorField]` IN ($placeholders) ORDER BY `$syncTable[mirrorField]`");
		$prepSelectNewIds->execute($mirrorValues);
		$newIds=$prepSelectNewIds->fetchAll(PDO::FETCH_COLUMN);
		return array_combine(array_keys($oldPkToMirror), $newIds);
	}
	
	/**
	 * 
	 * @param type $syncTable
	 * @param type $dbKey
	 * @param PDO $db
	 */
	protected function insertRows($syncTable,$dbKey,$db) {
		$cols_impl="`".implode("`,`",$syncTable[$dbKey]['columns'])."`";
		$colsPlaceholders='?'.str_repeat(',?', count($syncTable[$dbKey]['columns'])-1);
		$rows=$syncTable[$dbKey]['insertRows'];
		if (!empty($syncTable['fks'])) {//if this table has any fks that need to be translated before insertion
			foreach ($syncTable['fks'] as $columnName=>$referencedTable) {
				$columnIndex=array_search($columnName, $syncTable[$dbKey]['columns']);
				foreach ($rows as $rowIndex=>$row) {
					if ($row[$columnIndex]) {
						$rows[$rowIndex][$columnIndex]=
							$this->syncData[$referencedTable][$dbKey]['pkTranslation'][$row[$columnIndex]];
					}
				}
			}
		}
		$db->beginTransaction();
		while ($rowsPortion=array_splice($rows,0,10000)) {	
			$rowsPlaceholders="($colsPlaceholders)".str_repeat(",($colsPlaceholders)", count($rowsPortion)-1);
			$prepRowInsert=$db->prepare(
				"INSERT INTO `$syncTable[name]` ($cols_impl)".PHP_EOL
				."VALUES $rowsPlaceholders"
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
	
	/**
	 * 
	 * @param SynkaTable $table
	 * @param type $tableColumns
	 * @param type $dbKey
	 * @param type $type
	 * @param type $rows
	 */
	protected function addSyncData($table,$tableColumns,$dbKey,$type,$rows) {
			if (!empty($this->syncData[$table->tableName][$dbKey]['translateIds'])) {
				$pkIndex=array_search($table->pk, $tableColumns);
				unset($tableColumns[$pkIndex]);
				$this->syncData[$table->tableName][$dbKey]['translateInsertionIds']=self::unsetColumn2dArray($rows,$pkIndex);
			}
			
		$this->syncData[$table->tableName][$dbKey]['columns']=$tableColumns;
		$this->syncData[$table->tableName][$dbKey][$type]=$rows;
		
		if (!empty($table->linkedTables)) {
			foreach ($table->linkedTables as $referencedTableName=>$link) {
				$referencedTable=$this->syncData[$referencedTableName];
				if ($referencedTable['mirrorField']!==$referencedTable['pkField']) {
					$linkedColumn=$link['COLUMN_NAME'];
					$fkIndex=array_search($linkedColumn, $tableColumns);
					$translateIds=array_column($rows, $fkIndex);
					if (!(count($translateIds)===1&&!$translateIds[0])) {
						//do array_fill_keys because we want the ids as keys to avoid duplicates, and choose TRUE as
						//value instead of flipping and getting random integers.
						$this->syncData[$referencedTableName][$dbKey]['translateIds']
							+=array_fill_keys(array_column($rows, $fkIndex), TRUE);
					}
				}
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTable $table
	 * @return type
	 */
	protected function getFieldsToSelect($table,$dbKey) {
		$tableColumns=$table->columns;
		foreach ($tableColumns as $tableColumn) {
			if ($tableColumn['Field']!==$table->pk||$table->pk===$table->mirrorField
			||!empty($this->syncData[$table->tableName][$dbKey]['translateIds'])) {
				$fields[]=$tableColumn['Field'];
			}
		}
		return $fields;
	}
	
	protected function implodeTableFields($fields) {
		return "`".implode('`,`',$fields).'`';
	}
}