<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
class SynkaTable {
	public $tableName;
	public $mirrorField;
	public $columns;
	public $syncs=[];
	public $pk;
	public $mirrorPk=false;
	public $translateIds;
	
	/**List of tables that this table links to through foreign keys, if any.
	 * Key is the name of the referenced table, value is:
	 * array ["COLUM_NAME"=>string name of the fk-field in this table,
	 * "REFERENCED_COLUMN_NAME"=>string name of the field in the other table that the fk-field of this table points to]
	 * @var string[]*/
	public $linkedTables;
	
	public function __construct($tableName,$mirrorField,$columns,$linkedTables) {
		$this->tableName=$tableName;
		$this->mirrorField=$mirrorField;
		$this->columns=$columns;
		foreach ($columns as $column) {
			if ($column['Key']==='PRI'&&$column['Extra']==='auto_increment') {
				$this->pk=$column['Field'];
				if ($column['Field']===$mirrorField) {
					$this->mirrorPk=true;
				}
				break;
			}
		}
		$this->linkedTables=$linkedTables;
	}
	
	public function insertUnique() {
		if (!isset($this->mirrorField)) {
			trigger_error("Can't do insertUnique() on a table with no "
						. "specified mirrorField(specify it in in the Synka->table() method)");
		}
		$this->syncs['insertUnique']=true;
	}
	
	public function insertCompare($compareField,$compareOperator) {
		$this->syncs['insertCompare']=['compareField'=>$compareField,'compareOperator'=>$compareOperator];
	}
}