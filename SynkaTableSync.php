<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class SynkaTableSync {
	/**The table containing this sync
	 * @var SynkaTable */
	public $table;
	public $syncData;
	public $insertionIds;
	
	public $copyFields,$compareField,$compareOperator,$subsetField,$selectFields;
	public function __construct($table,$copyFields,$compareField,$compareOperator,$subsetField) {
		foreach (get_defined_vars() as $varName=>$val) {
			$this->$varName=$val;
		}
	}
}