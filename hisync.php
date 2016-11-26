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
class hisync {
	/**
	 *
	 * @var PDO[] 
	 */
	protected $dbs;
	protected $tableSyncs;
	public $syncData;
	
	/**Get's the MAX-values(highest) of $compareField from both tables, and syncs the rows having a $compareField
	 * which is higher than the lower one of the two max-values*/
	const COMP_ABOVE_MAX=0;
	const COMP_UNIQUE=1;
	function __construct($localDB,$remoteDB) {
		$this->localDB=$localDB;
		$this->remoteDB=$remoteDB;
		$this->tableSyncs=[];
		$this->dbs=['local'=>$localDB,'remote'=>$remoteDB];
	}
	
	public function addTableSyncInsert($tableName,$primaryKeyField,$referenceField,$fields,$foreignKeyFields
	,$compareField,$compareMethod) {
		$this->tableSyncs[]=['name'=>$tableName,'pk'=>$primaryKeyField,'ref'=>$referenceField,'fields'=>$fields
			,'fks'=>$foreignKeyFields,'compField'=>$compareField,'compMethod'=>$compareMethod,'type'=>0];
	}
	
	public function addTableSyncMultiKey($tableName,$primaryKeyFields,$fields,$foreignKeyFields
	,$compareField,$compareMethod) {
		$this->tableSyncs[]=['name'=>$tableName,'pk'=>$primaryKeyFields,'fields'=>$fields
			,'fks'=>$foreignKeyFields,'compField'=>$compareField,'compMethod'=>$compareMethod,'type'=>1];
	}
	
	public function compare() {
		$this->syncData=['local'=>[],'remote'=>[]];
		foreach ($this->tableSyncs as $tableSync) {
			switch ($tableSync['type']) {
				case 0://normal inserts
					switch ($tableSync['compMethod']) {
						case self::COMP_ABOVE_MAX:
							$this->compareAboveMax($tableSync);
						break;
						case self::COMP_UNIQUE:
							$this->compareUnique($tableSync);
					}
				break;
				case 1://multi-column-pk insert
					switch ($tableSync['compMethod']) {
						case self::COMP_UNIQUE:
							$this->compareUniqueMultiColumnPK($tableSync);
					}
					break;
			}
		}
		return $this->syncData;
	}
	
	/**
	 * 
	 * @param type $tableSync
	 */
	protected function compareAboveMax($tableSync) {
		$maxes=[];
		foreach ($this->dbs as $current=>$db) {
			$maxes[$current]=(int)$db->query("SELECT MAX(`$tableSync[compField]`) FROM `$tableSync[name]`")
				->fetch(PDO::FETCH_COLUMN);
		}
		if ($maxes['local']!==$maxes['remote']) {
			asort($maxes);
			$lowerDb=key($maxes);
			$higherDb=$lowerDb==='local'?'remote':'local';
			$this->syncData[$lowerDb][$tableSync['name']][]
				=["Insert rows from $higherDb where $tableSync[compField] is above $maxes[$lowerDb]"
				,0,$tableSync[compField],$maxes[$lowerDb]];
		}
	}
	
	protected function compareUnique($tableSync) {
		$values=[];
		foreach ($this->dbs as $which=>$db) {
			$values[$which]=$db->query("SELECT $tableSync[compField] FROM $tableSync[name]")
				->fetchAll(PDO::FETCH_COLUMN);
		}
		foreach ($values as $current=>$currentValues) {
			$other=$current==='remote'?'local':'remote';
			$otherValues=$values[$other];
			$valuesToSync=array_diff($otherValues, $currentValues);
			$numToSync=count($valuesToSync);
			if (!empty($valuesToSync)) {
				$this->syncData[$current][$tableSync['name']][]
					=["Insert $numToSync rows from the $other database which have values of $tableSync[compField]"
					. "not present on the $current database."
					,1,$tableSync[compField],$valuesToSync];
			}
		}
	}
}
