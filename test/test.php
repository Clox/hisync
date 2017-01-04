<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

//how it should be able to be used

require_once '../Synka.php';

$local=new PDO('mysql:host=localhost:3306;dbname=fundtracker;charset=utf8', 'fundtracker', 'blomma22');
$local->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$remote=new PDO('mysql:host=localhost:3306;dbname=fundtrackertest;charset=utf8', 'fundtracker', 'blomma22');
$remote->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
$synka=new Synka($local,$remote);

//needed because otherwise there will be an error when trying to access portfolios in syncData to place
//translateId's in it, but that's the only reason
$synka->table("portfolios","id");

$synka->table("funds","id")->insertCompare("id",">");

//since the funds-table was added with "id" as mirrorField, the below method will know that the FK values in
//fundratings that are pointing to funds are same on both sides and can just copy them directly rather than having to 
//link old ids to new ids
$synka->table("fundratings","id")->insertCompare("id",">");

$synka->table("portfolio_snapshots","id")->insertCompare("id",">");
$synka->table("portfolio_snapshots_in_portfolios","id")->insertCompare("id",">");

$synka->table("exchanges","exchangeName")->insertUnique();
$synka->table("tickers","tickerSymbol")->insertUnique();
$synka->table("securities","id")->insertCompare("id",">");
$synka->table("portfoliorows")->insertCompare("portfolioSnapshotId",">");


$synka->table("strategies","id")->insertUnique();


$synka->table("dividends")->insertCompare("time1",">");



//Finally process all of the added tableSyncs.
$syncData=$synka->compare();

$synka->sync();
exit;

//Synka->syncInsertUnique
//syncs rows of the specified table across the two sides where the specified field does not exist on the other side
//param 1 = tableName
//param 2 = the field that uniquely identifies each row and that should be the same on both sides, e.g. usually not an
//	auto-incremented field.
//param 3 = array of fields that should be excluded when copying from one side to the other

//Synka->syncInsertCompare
//compares the specified field of the rows on one side with the other to sync the ones that match according to the
//specified compareMethod and optionally subsetField
//param 1 = tableName - The name of the table to be synced
//param 2 = the field that should be compared across both sides
//param 3 = compareMethod. possible values are 
//				">" = any rows on side A in the specified table that have values of the
//				specified field higher than those on side B will be copied to side A
//param 4 "groupField" = if this is supplied than the comparison will be done on a subset,
//	grouped by the specified field

//Synka->syncInsertRowsOfNewFk
//copies rows of a table that has FK-key which points to rows that get created with this sync
//param 1 = tableName


$synka->syncInsertCompare("quotes","date",">","tickerId");
$synka->syncInsertCompare("splits","time1",">","tickerId");
$synka->syncUpdateInsert("tickers_in_strategies",["strategyId","tickerId"],"updatedAt",["included"]);
$synka->syncUpdate("securities","id","updatedAt",["tickerId","notes","ignored"]);


//Synka->addTable
//param 1 = tableName
//param 2 = array of the fields that are to be excluded

//Synka->addTable->syncInsert
//param 1 = field that uniquely identifies each row and which should be the same on both local and
//remote, e.g. not an auto-incremented field
//param 2 = field that also uniquely identifies the rows but which is auto-incremented. if this is supplied if this argument is not set
//	then the field of param 1 will be used instead. 

//sync
$synka->addTable("strategies",
	['id','name','description','factorConfigs','skipNumDays','numHoldings','factorConfigsOutcome','updatedOn'])
	->syncInsert("id","id");
$synka->addTable("exchanges",['timezone','gmtoffset','preOpenTime','preCloseTime'
							,'regularOpenTime','regularCloseTime','postOpenTime','postCloseTime'])
	->syncInsert();

$table2=$synka->addTable("tableName2",["field1","field2","field3"]);