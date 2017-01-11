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

//test local more manual
$remote=new PDO('mysql:host=localhost:3306;dbname=fundtrackertest;charset=utf8', 'fundtracker', 'blomma22');
$remote->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$synka=new Synka($local,$remote);
$synka->table("portfolios","id");
$synka->table("funds","id")->insertCompare("id",">");
$synka->table("fundratings","id")->insertCompare("id",">");
$synka->table("portfolio_snapshots","id")->insertCompare("id",">");
$synka->table("portfolio_snapshots_in_portfolios","id")->insertCompare("id",">");
$synka->table("exchanges","exchangeName")->insertUnique();
$synka->table("tickers","tickerSymbol")->insertUnique();
$synka->table("securities","id")->insertCompare("id",">")->update("updatedAt",">",["tickerId","notes","ignored"]);
$synka->table("portfoliorows")->insertCompare("portfolioSnapshotId",">");
$synka->table("strategies","id")->insertUnique();
$synka->table("dividends")->insertCompare("time1",">","tickerId");
$synka->table("splits")->insertCompare("time1",">","tickerId");
$synka->table("quotes")->insertCompare("date",">","tickerId");
$synka->table("tickers_in_strategies")->insertCompare("updatedAt",">","strategyId",true);
$syncData=$synka->compare();//optional, if not called explicitly then Synka->sync() will call it
$synka->sync();