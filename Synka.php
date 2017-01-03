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
	 * @return SynkaTable Returns a table-object which has methods for syncing data in that table.*/
	public function table($tableName,$mirrorField=null) {
		$table=$this->tables[]=new SynkaTable($tableName,$mirrorField);
		$table->columns=$this->dbs['local']->query("desc `$tableName`;")->fetchAll(PDO::FETCH_ASSOC);
		$table->linkedTables=$this->dbs['local']->query(
			"SELECT REFERENCED_TABLE_NAME".PHP_EOL
			."FROM information_schema.TABLE_CONSTRAINTS i".PHP_EOL
			."LEFT JOIN information_schema.KEY_COLUMN_USAGE k ON i.CONSTRAINT_NAME = k.CONSTRAINT_NAME".PHP_EOL
			."WHERE i.CONSTRAINT_TYPE = 'FOREIGN KEY'".PHP_EOL
			."AND i.TABLE_SCHEMA = DATABASE()".PHP_EOL
			."AND k.CONSTRAINT_SCHEMA = DATABASE()".PHP_EOL
			."AND k.TABLE_NAME = '$tableName';")->fetchAll(PDO::FETCH_COLUMN);
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
				$this->syncData[$table->tableName]['fields']=$tableFields;
				$this->syncData[$table->tableName]['insertRows'][$thisDbKey]=$thisMissingRows;
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
				$this->syncData[$table->tableName]['fields']=$tableFields;
				$this->syncData[$table->tableName]['insertRows'][$thisDbKey]=$thisMissingRows;
				break;
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
			if ($tableColumn['Field']===$table->mirrorField) {//Always add if this is in the case, even if its
						//an auto-incremented PK since it should be the same on both sides
				$addField=true;
			} else if (!($tableColumn['Key']==="PRI"&&$tableColumn['Extra']==="auto_increment")) {
				$addField=true;
			} else {//if auto-incremented PK
				//if another syncing table has a FK pointing to this table then we want to select the ID even if
				//its not going to be inserted into the other DB, but to translate the id of this table from that
				//of other db to this db when inserting as FK in the other table
				
			}
			$fields[]=$tableColumn['Field'];
		}
		return $fields;
	}
	
	protected function implodeTableFields($fields) {
		return "`".implode('`,`',$fields).'`';
	}
}