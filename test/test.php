<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

//how it should be able to be used

require_once './SynkaTester.php';

$local=new PDO('mysql:host=localhost:3306;dbname=fundtracker;charset=utf8', 'fundtracker', 'blomma22');
$local->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

//test local more manual
$remote=new PDO('mysql:host=localhost:3306;dbname=fundtrackertest;charset=utf8', 'fundtracker', 'blomma22');
$remote->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$synka=new SynkaTester($local,$remote);

$synka->table("portfolios","id");
$synka->table("funds","id")->sync(["*-","portfolioId"],"id",">");
$synka->table("fundratings","id")->sync("*","id",">");
$synka->table("portfolio_snapshots","id")->sync("*","id",">");
$synka->table("portfolio_snapshots_in_portfolios","id")->sync("*","id",">");
$synka->table("exchanges","exchangeName")->sync("*","exchangeName","!=");
$synka->table("tickers","tickerSymbol")->sync("*","tickerSymbol","!=");
$synka->table("securities","id")->sync("*","id",">")->sync(["notes","ignored","tickerId"],"updatedAt",">","id");
$synka->table("portfoliorows")->sync("*","portfolioSnapshotId",">");
$synka->table("strategies","id")->sync("*","id","!=")->sync("*","updatedOn",">","id");
$synka->table("dividends")->sync("*","time1",">","tickerId");
$synka->table("splits")->sync("*","time1",">","tickerId");
$synka->table("quotes")->sync("*","date",">","tickerId");
$synka->table("tickers_in_strategies")
		->sync("*",["strategyId","tickerId"],"!=")->sync(["included"],"updatedAt",">",["strategyId","tickerId"]);
//$synka->table("tickers_in_strategies")->sync("*",["strategyId","tickerId"],"!=")->update(["included"],"updatedAt");

$syncData=$synka->analyze(false);//optional, if not called explicitly then Synka->sync() will call it
exit;
//$synka->sync();

//echo $synka->checkForDiscrepancies()?"match":"mismatch";