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
	public $syncs=[];
	public $pk;
	public $translateIds;
	public $syncData;
	public $fks=[];
	public $idForSelect;
	public $updateOnDupeKey;
	public $updateCols;
	
	/**List of tables that this table links to through foreign keys, if any.
	 * Key is the name of the referenced table, value is:
	 * array ["COLUM_NAME"=>string name of the fk-field in this table,
	 * "REFERENCED_COLUMN_NAME"=>string name of the field in the other table that the fk-field of this table points to]
	 * @var string[]*/
	public $linkedTables;
	
	public function __construct($tableName,$mirrorField,$columns,$linkedTables) {
		$this->tableName=$tableName;
		$this->mirrorField=$mirrorField;
		$this->columns=$columns;
		foreach ($columns as $column) {
			if ($column['Key']==='PRI'&&$column['Extra']==='auto_increment') {
				$this->pk=$column['Field'];
			}
		}
		$this->linkedTables=$linkedTables;
		$this->syncData=  array_fill_keys(["local","remote"], ['pkTranslation'=>[],'translateIds'=>[]]);
	}
	
	public function insertUnique() {
		if (!isset($this->mirrorField)) {
			trigger_error("Can't do insertUnique() on a table with no "
						. "specified mirrorField(specify it in in the Synka->table() method)");
		}
		$this->syncs['insertUnique']=true;
		return $this;
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
}