<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class SynkaTableColumn {
	public $name;
	public $type;
	public $key;
	public $extra;
	public $mirror;
	
	/**If this column is a FK-key then this should be set to the name of the table its referring to
	 * @var string*/
	public $fk;
	
	/**If this is either a unique coumn or a non auto incremented PK, and a global unique sync with this as 
	 * (one of) its compare-field(s) then when it fetches all the unique values to compare with this property should be
	 * populated with all of the unique values in the appropriate sub-array (local/remote). This is so that when when
	 * rows are being copied to the table it is known without doing an extra query whether they should be inserted
	 * because the unique value doesn't already exist, or used to updated since they already exist
	 * @var string[][] */
	public $allUniques=['local'=>[],'remote'=>[]];
	public function __construct($name,$type,$key,$extra,$mirror) {
		foreach (get_defined_vars() as $varName=>$val) {
			$this->$varName=$val;
		}
	}
}