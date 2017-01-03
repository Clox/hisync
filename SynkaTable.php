<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
class SynkaTable {
	protected $dbs;
	protected $tableName;
	protected $mirrorField;
	protected $columns;
	
	/** @var Synka Reference to the Synka-object holding this table.*/
	protected $synka;
	
	public function __construct($synka,$dbs,$tableName,$mirrorField) {
		$this->synka=$synka;
		$this->dbs=$dbs;
		$this->tableName=$tableName;
		$this->columns=$this->dbs['local']->query("desc `$tableName`;")->fetchAll(PDO::FETCH_ASSOC);
		$this->mirrorField=$mirrorField;
	}
	
	public function insertUnique() {
		if (!isset($this->mirrorField)) {
			trigger_error("Can't do insertUnique() on a table with no "
						. "specified mirrorField(specify it in in the Synka->table() method)");
		}
		$tableFields=$this->getFieldsToCopy();
		$tableFields_impl=$this->implodeTableFields($tableFields);
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
			$thisUniqueValues=$thisDb->query("SELECT `$this->mirrorField` FROM `$this->tableName`")
				->fetchAll(PDO::FETCH_COLUMN);
			$selectMissingRowsQuery="SELECT $tableFields_impl FROM `$this->tableName`";
			if (!empty($thisUniqueValues)) {
				$thisUniqueValuesPlaceholders="?".str_repeat(",?", count($thisUniqueValues)-1);
				$selectMissingRowsQuery.=PHP_EOL."WHERE `$this->mirrorField` NOT IN ($thisUniqueValuesPlaceholders)";
			}
			$prepSelectMissingRows=$otherDb->prepare($selectMissingRowsQuery);	
			$prepSelectMissingRows->execute($thisUniqueValues);
			$thisMissingRows=$prepSelectMissingRows->fetchAll(PDO::FETCH_NUM);
			if (!empty($thisMissingRows)) {
				$this->synka->syncData[$this->tableName]['fields']=$tableFields;
				$this->synka->syncData[$this->tableName]['insertRows'][$thisDbKey]=$thisMissingRows;
			}
		}
	}
	
	public function insertCompare($compareField,$compareOperator) {
		$tableFields=$this->getFieldsToCopy();
		$tableFields_impl=$this->implodeTableFields($tableFields);
		foreach ($this->dbs as $thisDbKey=>$thisDb) {
			$otherDb=$thisDb===$this->dbs['local']?$this->dbs['remote']:$this->dbs['local'];
			$thisExtremeValue=$thisDb->query("SELECT MAX(`$compareField`) FROM `$this->tableName`")
					->fetch(PDO::FETCH_COLUMN);
			$thisMissingRows=$otherDb->query("SELECT $tableFields_impl".PHP_EOL
				."FROM `$this->tableName` WHERE `$compareField`$compareOperator$thisExtremeValue")
				->fetchAll(PDO::FETCH_NUM);
			if (!empty($thisMissingRows)) {
				$this->synka->syncData[$this->tableName]['fields']=$tableFields;
				$this->synka->syncData[$this->tableName]['insertRows'][$thisDbKey]=$thisMissingRows;
				break;
			}
		}
	}
	
	protected function getTableColumns() {
		if (!isset($this->columns)) {
			$this->columns=$this->dbs['local']->query("SHOW columns FROM `$tableName`;")
				->fetchAll(PDO::FETCH_ASSOC);
		}
		return $this->columns;
	}
	
	protected function getFieldsToCopy() {
		$tableColumns=$this->getTableColumns();
		foreach ($tableColumns as $tableColumn) {
			if ($tableColumn['Field']===$this->mirrorField||
			!($tableColumn['Key']==="PRI"&&$tableColumn['Extra']==="auto_increment")) {
				$fields[]=$tableColumn['Field'];
			}
		}
		return $fields;
	}
	
	protected function implodeTableFields($fields) {
		return "`".implode('`,`',$fields).'`';
	}
}