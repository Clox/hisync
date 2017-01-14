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
	public $updateOnDupeKey;
	public $updateCols;
	public $translateIdFrom=['local'=>[],'remote'=>[]];
	
	/**List of tables that this table links to through foreign keys, if any.
	 * Key is the name of the referenced table, value is:
	 * array ["COLUM_NAME"=>string name of the fk-field in this table,
	 * "REFERENCED_COLUMN_NAME"=>string name of the field in the other table that the fk-field of this table points to]
	 * @var string[]*/
	public $linkedTables;
	
	public $mirrorField;
	
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
	
	public function insertUnique() {
		if (!isset($this->mirrorField)) {
			trigger_error("Can't do insertUnique() on a table with no "
						. "specified mirrorField(specify it in in the Synka->table() method)");
		}
		$this->syncs['insertUnique']=true;
		return $this;
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
		}
		if (is_string($subsetField)) {
			$subsetField=[$subsetField];
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
		$this->syncs[]=new SynkaTableSync($this,$fields, $compareField, $compareOperator, $subsetField);
		return $this;
	}
	
	public function syncCompare($fields,$compareField,$compareOperator=">") {
		if ($fields[0]==="*-") {
			$fields=array_intersect(array_keys($this->columns));
		} else if ($fields==="*") {
			$fields=array_keys($this->columns);
		}
		$type='compare';
		$this->syncs[]=compact('type','fields','compareField','compareOperator');
	}
	
	public function syncUnique() {
		
	}
	
	/**Adds a row-insertion-sync to the table which identifies the rows that should be copied by comparing the field
	 * specified as $compareField between the two databases, any rows that have a value of that field higher than the
	 * max of the other, or lower than the minimum of the other depending on $compareOperator are going to be copied.
	 * If $subsetField is set then only rows with higher/lower values of $compareField within the same value of
	 * $subsetField are going to be copied.
	 * @param string $compareField The field to do the comparison on
	 * @param string $compareOperator Eiher "<" for copying rows that are of lower values, or ">" for higher
	 * @param string $subsetField If this is set then the comparisons are done on a per subset basis, grouped by
	 *		the specified field. If null then only one comparison is done, on the whole table.*/
	public function insertCompare($compareField,$compareOperator,$subsetField=null,$updateOnDupeKey=false) {
		$this->updateOnDupeKey=$updateOnDupeKey;
		$this->syncs['insertCompare']=compact("compareField","compareOperator","subsetField");
		return $this;
	}
	
	/**Tells Synka to do row-updating on the referenced table. The table must have a PK-column for it to work.
	 * It works similarely to insertCompare in that it needs a field to do comparison on, to identify which side
	 * the row is most up to date on and should be copied from.
	 * Common usage would be something like update("updatedAt",">",["foo"]) given that the table has an
	 * "updatedAt"-field which gets set to current timestamp when the "foo"-field is updated. It is also important
	 * that the "updatedAt"-field cannot be null or otherwise the comparison wont work correctly.
	 * @param string $compareField A field-name to do comparison on. Typically "updatedAt"
	 * @param string $compareOperator Either "<" for replacing the row with the higher value of "$compareField" or ">"
	 *									for the opposite which would be the most common one.
	 * @param string[] $updateFields A list of columns that should be updated.
	 * @return \SynkaTable*/
	public function update($compareField,$compareOperator,$updateFields) {
		$updateFields[]=$this->pk;
		if (!in_array($compareField, $updateFields)) {
			$updateFields[]=$compareField;
		}
		$this->updateCols=$updateFields;
		$this->syncs['update']=compact("compareField","compareOperator");
		return $this;
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