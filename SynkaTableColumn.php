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
	
	public function __construct($name,$type,$key,$extra,$mirror) {
		foreach (get_defined_vars() as $varName=>$val) {
			$this->$varName=$val;
		}
	}
}