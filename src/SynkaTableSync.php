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
	
	/**See Synka->setTableSyncsSelectFields() for explanation.
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
		//$hasNonCopiedPk, $copyingNonComparedPk, $copyingNonComparedUnique=
		$hasNonCopiedPk=$table->pk&&!in_array($table->pk, $copyFields);
		$copyingNonComparedPk=$table->pk&&!in_array($table->pk, $compareFields)&&in_array($table->pk, $copyFields);
		$copyingNonComparedUnique=null;
		foreach ($copyFields as $colName) {
			if ($table->columns[$colName]->key==="UNI"&&!in_array($colName, $compareFields)) {
				$copyingNonComparedUnique=true;
				break;
			}
		}
		if (!$copyingNonComparedPk&&!$copyingNonComparedUnique) {
			$this->copyStrategy=1;
		} else if ($table->pk&&!$hasNonCopiedPk) {
			$this->copyStrategy=2;
		} else {//} else if ($hasNonCopiedPk&&$copyingNonComparedUnique) {
			$this->copyStrategy=3;
		}
		$a=1;
	}
}