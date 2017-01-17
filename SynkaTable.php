<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
require_once 'SynkaTableSync.php';
require_once 'SynkaTableColumn.php';
class SynkaTable {
	public $tableName;
	
	/**Array of columns where key is column-name
	 * @var SynkaTableColumn */
	public $columns;
	
	/**List of syncs that should be done on this table. Added via SynkaTable->addSync()
	 * @var SynkaTableSync[] */
	public $syncs=[];
	
	public $pk;
	public $translateIdFrom=['local'=>[],'remote'=>[]];
	
	/**List of tables that this table links to through foreign keys, if any.
	 * Key is the name of the referenced table, value is:
	 * array ["COLUM_NAME"=>string name of the fk-field in this table,
	 * "REFERENCED_COLUMN_NAME"=>string name of the field in the other table that the fk-field of this table points to]
	 * @var string[]*/
	public $linkedTables;
	
	public $mirrorField;
	
	/**When sync() is called, the $compareField argument is saved in this variable, and is used for syncUpd()
	 * @var string[]*/
	protected $lastCompareField;
	
	/**If this is either a unique coumn or a non auto incremented PK, and a global unique sync with this as 
	 * (one of) its compare-field(s) then when it fetches all the unique values to compare with this property should be
	 * populated with all of the unique values in the appropriate sub-array (local/remote). This is so that when when
	 * rows are being copied to the table it is known without doing an extra query whether they should be inserted
	 * because the unique value doesn't already exist, or used to updated since they already exist
	 * @var string[][] */
	public $allUniques=['local'=>[],'remote'=>[]];
	
	public function __construct($tableName,$mirrorField,$columns,$linkedTables) {
		$this->tableName=$tableName;
		$this->mirrorField=$mirrorField;
		foreach ($columns as $column) {
			if ($column['key']==='PRI') {
				$this->pk=$column['name'];
			}
			$this->columns[$column['name']]=new SynkaTableColumn
					($column['name'],$column['type'],$column['key'],$column['extra'],$mirrorField===$column['name']);
		}
		$this->linkedTables=$linkedTables;
	}
	
	/**Adds a sync to the table which will be processed when calling Synka->compare() and Synka->
	 * @param array|string $fields Specifies which fields to be copied. Can be "*" for all fields, or an array of
	 *		field-name-strings. If the first element of the array is "*-" then it means all fields except the ones
	 *		specified in the rest of the elements will be copied.
	 *		The field specified as $compareField will always be included no matter what.
	 * @param string|array $compareField The field that comparison will be done on, to identify what rows should be
	 *		copied. Might for instance be "updatedAt" with $compareOperator as ">", or "userName" with
	 *		$compareOperator as "!=". It can also be an array of multiple field-strings is $compareOperator is "!="
	 * @param string $compareOperator "<"|">"|"!="<ul>
	 *		<li>"<" is for copying rows from the source DB with values of $compareField lower than the minimum of the
	 *		target DB.</li>
	 *		<li>">" is the reverse of "<". Commonly used while $compareField is something like addedAt or updatedAt
	 *			for copying new updates</li>
	 *		<li>"!=" is for finding unique values of $compareField. An example of it would be to use it while
	 *			$compareField is i.e. "userName" which should be a unique field.</li></ul>
	 * @param string|array $subsetField An optional string of a field which groups together rows with the same value of it
	 *		to use the $compareField and $compareOperator within each group rather than globally.
	 * @return SynkaTable Returns the table-object to enable adding multiple syncs in a chain.*/
	public function sync($fields,$compareField,$compareOperator,$subsetField=null) {
		if (is_string($compareField)) {
			$compareField=[$compareField];
		} else {
			sort($compareField);
		}
		if (is_string($subsetField)) {
			$subsetField=[$subsetField];
		} else if (isset($subsetField)) {
			sort($subsetField);
		}
		if ($fields[0]==="*-") {
			$fields=array_diff(array_keys($this->columns),$fields);
		} else if ($fields==="*") {
			$fields=array_keys($this->columns);
			if ($this->pk&&$this->columns[$this->pk]->extra==="auto_increment"&&!$this->columns[$this->pk]->mirror) {
				array_splice($fields, array_search($this->pk, $fields),1);
			}
		}
		foreach ($compareField as $singleCompareField) {
			if (!in_array($singleCompareField, $fields)) {
				$fields[]=$compareField[0];
			}
		}
		if ($subsetField) {
			foreach ($subsetField as $singleSubsetField) {
				if (!in_array($singleSubsetField,$fields)) {
					$fields[]=$singleSubsetField;
				}
			}
		}
		$this->lastCompareField=$compareField;
		$this->syncs[]=new SynkaTableSync($this,$fields, $compareField, $compareOperator, $subsetField);
		return $this;
	}
	
	/**This is a helper-funktion for sync(). It can only be called after sync() has been called, and then it will
	 * internally call sync as:<ol>
	 * <li>$fields being what is specified as $fields here</li>
	 * <li>$compareField being what is specified as $updatedAtField here</li>
	 * <li>$compareOperator as what is specified as $compareOperator here, but default is ">"</li>
	 * <li>$subsetField as what was used as $compareField in the last call of sync()</li><ol>
	 * This can be usefull when first having a sync-call adding new rows, then a syncUpd-call updating old ones.
	 * @param string[] $fields See $fields in sync()
	 * @param string $updatedAtField Same as $compareField in sync()
	 * @param string $compareOperator Same as $compareOperator in sync()
	 * @return SynkaTable*/
	public function syncUpd($fields,$updatedAtField,$compareOperator=">") {
		return $this->sync($fields, $updatedAtField, $compareOperator, $this->lastCompareField);
	}
	
	/**
	 * 
	 * @param type $side
	 * @param type $ids
	 */
	public function addIdsToTranslate($side,$ids) {
		$this->translateIdFrom[$side]+=array_fill_keys($ids, null);
	}
}