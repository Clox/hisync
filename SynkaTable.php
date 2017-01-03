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
	public $syncs;
	
	/**List of tables that this table links to through foreign keys, if any.
	 * @var string[]*/
	public $linkedTables;
	
	public function __construct($tableName,$mirrorField) {
		$this->tableName=$tableName;
		$this->mirrorField=$mirrorField;
		$this->syncs=[];
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