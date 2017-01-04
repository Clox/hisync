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
	
	public $syncData;
	
	/**
	 *
	 * @var SynkaTable[] 
	 */
	protected $tables;
	
	/**
	 * 
	 * @param PDO $localDB
	 * @param PDO $remoteDB
	 */
	public function __construct($localDB,$remoteDB) {
		$this->tables=[];
		$this->syncData=[];
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
		$exceptColumns="";
		if (!empty($ignoredColumns)) {
			$ignoredColumns_impl="'".implode("','",$ignoredColumns)."'";
			$exceptColumns=PHP_EOL."AND COLUMN_NAME NOT IN ($ignoredColumns_impl)";
		}
		$columns=$this->dbs['local']->query(
			"SELECT COLUMN_NAME Field,COLUMN_KEY 'Key',EXTRA Extra".PHP_EOL
			."FROM INFORMATION_SCHEMA.COLUMNS".PHP_EOL
			."WHERE TABLE_SCHEMA=DATABASE()".PHP_EOL
			."AND TABLE_NAME='$tableName'"
			.$exceptColumns
			)->fetchAll(PDO::FETCH_ASSOC);
		$linkedTables=$this->dbs['local']->query(
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
		
		$this->syncData[$tableName]=['mirrorPk'=>$table->mirrorPk];
		
		return $table;
	}
	
	public function compare() {
		foreach ($this->tables as $table) {
			$syncs=$table->syncs;
			if (key_exists('insertUnique', $syncs)) {
				$this->insertUnique($table);
			} else if (key_exists('insertCompare', $syncs)) {
				$sync=$syncs['insertCompare'];
				$this->insertCompare($table,$sync['compareField'],$sync['compareOperator']);
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTable $table
	 */
	protected function insertUnique($table) {
		$tableFields=$this->getFieldsToSelect($table);
		$tableFields_impl=$this->implodeTableFields($tableFields);
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
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
		$tableFields=$this->getFieldsToSelect($table);
		$tableFields_impl=$this->implodeTableFields($tableFields);
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
			$thisExtremeValue=$thisDb->query("SELECT MAX(`$compareField`) FROM `$table->tableName`")
					->fetch(PDO::FETCH_COLUMN);
			$thisMissingRows=$otherDb->query("SELECT $tableFields_impl".PHP_EOL
				."FROM `$table->tableName` WHERE `$compareField`$compareOperator$thisExtremeValue")
				->fetchAll(PDO::FETCH_NUM);
			if (!empty($thisMissingRows)) {
				$this->addSyncData($table,$tableFields,$thisDbKey,"insertRows",$thisMissingRows);
				break;
			}
		}
	}
	
	/**
	 * 
	 * @param SynkaTable $table
	 * @param type $tableFields
	 * @param type $dbKey
	 * @param type $type
	 * @param type $rows
	 */
	protected function addSyncData($table,$tableFields,$dbKey,$type,$rows) {
		$this->syncData[$table->tableName]['fields']=$tableFields;
		$this->syncData[$table->tableName][$dbKey][$type]=$rows;
		
		if (!empty($table->linkedTables)) {
			foreach ($table->linkedTables as $referencedTableName=>$link) {
				if (!$this->syncData[$referencedTableName]['mirrorPk']) {
					$linkedColumn=$link['COLUMN_NAME'];
					foreach ($table->columns as $columnIndex=>$column) {
						if ($column['Field']===$linkedColumn) {
							break;
						}
					}
					$translateIds=array_column($rows, $columnIndex);
					if (!(count($translateIds)===1&&!$translateIds[0])) {
						//do array_fill_keys because we want the ids as keys to avoid duplicates, and choose TRUE as
						//value instead of flipping and getting random integers.
						$translateIds=array_fill_keys(array_column($rows, $columnIndex), TRUE);
						if (isset($this->syncData[$referencedTableName][$dbKey]['translateIds'])) {
							$this->syncData[$referencedTableName][$dbKey]['translateIds']+=$translateIds;
						} else {
							$this->syncData[$referencedTableName][$dbKey]['translateIds']=$translateIds;
						}
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
	protected function getFieldsToSelect($table) {
		$tableColumns=$table->columns;
		foreach ($tableColumns as $tableColumn) {
			$addField=false;
			if (!($tableColumn['Key']==="PRI"&&$tableColumn['Extra']==="auto_increment")||$table->mirrorPk) {
				$addField=true;
			} else {
				//if another syncing table has a FK pointing to this table then we want to select the ID even if
				//its not going to be inserted into the other DB, but to translate the id of this table from that
				//of other db to this db when inserting as FK in the other table
				foreach ($this->tables as $otherTable) {
					if ($otherTable!==$table&&isset($otherTable->linkedTables[$table->tableName])) {
						$addField=true;
					}
				}
			}
			if ($addField) {
				$fields[]=$tableColumn['Field'];
			}
		}
		return $fields;
	}
	
	protected function implodeTableFields($fields) {
		return "`".implode('`,`',$fields).'`';
	}
}