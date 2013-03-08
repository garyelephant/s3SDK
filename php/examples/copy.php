<?php
require("../SinaStorageService.php");
require_once( "conf.php" );

$o = SinaStorageService::getInstance($project, $accesskey, $secretkey);

//可自定义配置的CURLOPT
$o->setCURLOPTs(array(CURLOPT_VERBOSE=>1));
$o->setRequestHeaders(array("fortest"=>1));

//设置使用验证方式
$o->setAuth(true);

$rc = $o->copyFile("1123.html","123.html", $result);
echo $result;
