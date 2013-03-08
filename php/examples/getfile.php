<?php
require("../SinaStorageService.php");
require_once( "conf.php" );

$o = SinaStorageService::getInstance($project, $accesskey, $secretkey);

//可自定义配置的CURLOPT
$o->setCURLOPTs(array(CURLOPT_VERBOSE=>1));
//$o->setQueryStrings(array("ip"=>"1404667596,161.135.152.194"));

//设置使用验证方式
$o->setAuth(true);

var_dump($o->getFile("foo/bar/1.html",$result));
echo $result;

// TODO add anonymmous access example
