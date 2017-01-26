<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
require_once __DIR__.'/../src/Synka.php';

class SynkaTester extends Synka {
	public function checkForDiscrepancies() {
		$data=[];
		$orderedTables=[];
		foreach ($this->dbs as $currentSide=>$currentDb) {
			foreach ($this->tables as $tableName=>$table) {
				$fields=[];
				$orderBy="";
				if ($table->pk&&$table->pk===$table->mirrorField) {
					$orderBy="ORDER BY `$table->pk`";
					$orderedTables[$tableName]=true;
				}
				foreach ($table->columns as $column) {
					if (!in_array($column->name, ['lastPortfolioScanTime','lastAttemptedPortfolioScan']))
					$fields[]=$column->name;
					if ($column->key==="UNI"&&!$column->fk) {
						$orderBy="ORDER BY `$column->name`";
						$orderedTables[$tableName]=true;
					}
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
				$fields_impl=$this->implodeTableFields($fields,$table);
				echo "\nFetch $tableName from $currentSide...";
				$tableData=$currentDb->query("SELECT $fields_impl FROM $tableName $orderBy")
					->fetchAll($fetchMode);
				echo " done!";
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
		echo "\nAll data fetched.";
		foreach (["local","remote"] as $currentSide) {
			foreach ($data[$currentSide] as $tableName=>$table) {
				$data[$currentSide][$tableName]=array_values($table);
				if (!isset($orderedTables[$tableName])) {
					sort ($data[$currentSide][$tableName]);
				}
			}
		}
		$match=$data['local']==$data['remote'];
		if (!$match) {
			foreach ($data['local'] as $localTableName=>$localTable) {
				if ($localTable!=$data['remote'][$localTableName]) {
					foreach ($localTable as $localRowIndex=>$localRow) {
						$remoteRow=$data['remote'][$localTableName][$localRowIndex];
						if ($localRow!==$remoteRow) {
							foreach ($localRow as $fieldName=>$localField) {
								$remoteField=$remoteRow[$fieldName];
								if ($localField!==$remoteField) {
									echo "Mismatch at:";
									print_r(['table'=>$localTableName,'row'=>$localRowIndex,'field'=>$fieldName]);
									die;
								}
							}
						}
					}
				}
			}
		} else {
			echo "match!!!";
			die;
		}
		
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