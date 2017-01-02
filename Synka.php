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
class Synka {
	/** @var PDO */
	protected $localDb;
	
	/** @var PDO */
	protected $remoteDb;
	
	/**@var PDO[] */
	protected $dbs;//an array of the two above variables, keys being local & remote
	
	protected $tableColumns;
	
	public $syncData;
	
	/**
	 * 
	 * @param PDO $localDB
	 * @param PDO $remoteDB
	 */
	public function __construct($localDB,$remoteDB) {
		$this->localDb=$localDB;
		$this->remoteDb=$remoteDB;
		$this->syncEntries=$this->tableColumns=[];
		$this->dbs=['local'=>$localDB,'remote'=>$remoteDB];
	}
	
	public function syncInsertUnique($tableName, $uniqueField) {
		$tableFields_impl=$this->getFieldsToCopy($tableName);
		$otherDb=$this->remoteDb;
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->localDb?$this->remoteDb:$this->localDb;
			$thisUniqueValues=$thisDb->query("SELECT `$uniqueField` FROM `$tableName`")
				->fetchAll(PDO::FETCH_COLUMN);
			$selectMissingRowsQuery="SELECT $tableFields_impl FROM `$tableName`";
			if (!empty($thisUniqueValues)) {
				$thisUniqueValuesPlaceholders="?".str_repeat(",?", count($thisUniqueValues)-1);
				$selectMissingRowsQuery.=PHP_EOL."WHERE `$uniqueField` NOT IN ($thisUniqueValuesPlaceholders)";
			}
			$prepSelectMissingRows=$otherDb->prepare($selectMissingRowsQuery);	
			$prepSelectMissingRows->execute($thisUniqueValues);
			$thisMissingRows=$prepSelectMissingRows->fetchAll(PDO::FETCH_NUM);
			if (!empty($thisMissingRows)) {
				$this->syncData[$thisDbKey][$tableName][]
					=['type'=>'insert','fields'=>$tableFields_impl,'rows'=>$thisMissingRows];
			}
		}
	}
	
	public function syncInsertCompare($tableName,$compareField,$compareOperator) {
		$tableFields_impl=$this->getFieldsToCopy($tableName,$compareField);
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->localDb?$this->remoteDb:$this->localDb;
			$thisExtremeValue=$thisDb->query("SELECT MAX(`$compareField`) FROM `$tableName`")->fetch(PDO::FETCH_COLUMN);
			$thisMissingRows=$otherDb->query("SELECT $tableFields_impl".PHP_EOL
				."FROM `$tableName` WHERE `$compareField`>$thisExtremeValue")->fetchAll(PDO::FETCH_NUM);
			if (!empty($thisMissingRows)) {
				$this->syncData[$thisDbKey][$tableName][]
					=['type'=>'insert','fields'=>$tableFields_impl,'rows'=>$thisMissingRows];
				break;
			}
		}
	}
	
	protected function getTableColumns($tableName) {
		if (!key_exists($tableName, $this->tableColumns)) {
			$this->tableColumns[$tableName]=$this->localDb->query("SHOW columns FROM `$tableName`;")
					->fetchAll(PDO::FETCH_ASSOC);
		}
		return $this->tableColumns[$tableName];
	}
	
	protected function getFieldsToCopy($tableName,$alwaysAdd=null) {
		$tableColumns=$this->getTableColumns($tableName);
		$tableFields_impl="";
		foreach ($tableColumns as $tableColumn) {
			if ($tableColumn['Field']===$alwaysAdd
			||!($tableColumn['Key']==="PRI"&&$tableColumn['Extra']==="auto_increment")) {
				if (!empty($tableFields_impl))
					$tableFields_impl.=",";
				$tableFields_impl.="`$tableColumn[Field]`";
			}
		}
		return $tableFields_impl;
	}
	
}