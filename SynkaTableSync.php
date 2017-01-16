<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class SynkaTableSync {
	/**The table containing this sync
	 * @var SynkaTable*/
	public $table;
	
	public $syncData;
	
	/**See Synka->setTableSyncsSelectFields() for explenation.
	 * @var int[]*/
	public $insertsSourceIds=['local'=>[],'remote'=>[]];
	
	/**Read-property which defines what strategy should be used to insert the fetched rows into the target DB.<ol>
	 * <li> 1 - simple insert</li>
	 * <li> 2 - insert with on duplicate key update</li>
	 * <li> 3 - need to find out which rows already exist, insert those that don't and update the rest</li>
	 * @var int*/
	public $copyStrategy;
	
	
	public $copyFields,$compareFields,$compareOperator,$subsetFields,$selectFields;
	public function __construct($table,$copyFields,$compareFields,$compareOperator,$subsetFields) {
		foreach (get_defined_vars() as $varName=>$val) {
			$this->$varName=$val;
		}
		$copyingUnique=$copyingPk=null;
		$hasNonComparedPk=$table->pk&&!in_array($table->pk,$compareFields);
		foreach ($copyFields as $colName) {
			$colInfo=$table->columns[$colName];
			if ($colInfo->key==="UNI") {
				$copyingUnique=true;
			} else if ($colInfo->key==="PRI") {
				$copyingPk=true;
			}
		}
		if (!$hasNonComparedPk&&!$copyingUnique) {
			$this->copyStrategy=1;
		} else if ($copyingPk||(!$hasNonComparedPk&&$copyingUnique)) {
			$this->copyStrategy=2;
		} else {//if ($table->pk&&!$copyingPk&&$copyingUnique)
			$this->copyStrategy=3;
		}
	}
}