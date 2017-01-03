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
	/** @var PDO */
	protected $localDb;
	
	/** @var PDO */
	protected $remoteDb;
	
	/**@var PDO[] */
	protected $dbs;//an array of the two above variables, keys being local & remote
	
	protected $tableColumns;
	
	public $syncData;
	
	protected $tables;
	
	/**
	 * 
	 * @param PDO $localDB
	 * @param PDO $remoteDB
	 */
	public function __construct($localDB,$remoteDB) {
		$this->localDb=$localDB;
		$this->remoteDb=$remoteDB;
		$this->tables=[];
		$this->syncEntries=$this->tableColumns=[];
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
		return $tables[]=new SynkaTable($this,$this->dbs,$tableName,$mirrorField);
	}
}