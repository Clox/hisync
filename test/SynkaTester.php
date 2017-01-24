<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
require_once '../src/Synka.php';

class SynkaTester extends Synka {
	public function checkForDiscrepancies() {
		$data=[];
		foreach ($this->dbs as $currentSide=>$currentDb) {
			foreach ($this->tables as $tableName=>$table) {
				$fields=[];
				foreach ($table->columns as $column) {
					$fields[]=$column->name;
				}
				$fetchMode=PDO::FETCH_ASSOC;
				$pk=null;
				foreach ($table->columns as $column) {
					if ($column->key==="PRI") {
						if ($pk) {//only for single-column pk
							$pk=null;
							break;
						}
						$pk=$column->name;
					}
				}
				if ($pk) {
					$pkIndex=  array_search($pk, $fields);
					array_splice($fields, $pkIndex,1);
					array_unshift($fields, $pk);
					$fetchMode|=PDO::FETCH_UNIQUE|PDO::FETCH_GROUP;
				}
				$fields_impl=$this->implodeTableFields($fields);
				$tableData=$currentDb->query("SELECT $fields_impl FROM $tableName")->fetchAll($fetchMode);
				if ($tableData) {
					foreach ($table->linkedTables as $linkedTableName=>$tableLink) {
						foreach ($tableData as $rowIndex=>$row) {
							$fkId=$row[$tableLink['colName']];
							if ($fkId) {
								$tableData[$rowIndex][$tableLink['colName']]=
									$data[$currentSide][$linkedTableName][$fkId];
							}
						}
					}
				}
				$data[$currentSide][$tableName]=$tableData;
			}
		}
		foreach (["local","remote"] as $currentSide) {
			foreach ($data[$currentSide] as $tableName=>$table) {
				$data[$currentSide][$tableName]=array_values($table);
			}
		}
		if ($data['local']!==$data['remote']) {
			foreach ($data['local'] as $localTableName=>$localTable) {
				if ($localTable!=$data['remote'][$localTableName]) {
					foreach ($localTable as $localRowIndex=>$localRow) {
						if ($localRow!==$data['remote'][$localTableName][$localRowIndex]) {
							echo "Mismatch at table \"$localTableName\", row $localRowIndex";
							die;
						}
					}
				}
			}
		}
		$match=$data['local']==$data['remote'];
		return $match;
		/*
		foreach ($data['local'] as $tableName=>$localTable) {
			$remoteTable=$data['remote'][$tableName];
			//$tableMatch=$localTable==$remoteTable;
			foreach ($localTable as $rowIndex=>$localRow) {
				$remoteRow=$remoteTable[$rowIndex];
				$rowMatch=$remoteRow==$localRow;
				if (!$rowMatch) {
					$a=1;
				}
			}
			continue;
		}
		 */
	}
}