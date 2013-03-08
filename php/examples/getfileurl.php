<?php
require("../SinaStorageService.php");
require_once( "conf.php" );


$o = SinaStorageService::getInstance($project, $accesskey, $secretkey);

$o->setAuth(true);
//可自定义配置的CURLOPT
$o->setCURLOPTs(array(CURLOPT_VERBOSE=>1));
$o->getFileUrl("foo/bar/1.html",$result);
echo $result,"\n";


//设置使用验证方式
//$o->setAuth(true);

//设置IP访问权限,获取一个只允许以下IP访问的url
$o->setExtra("?ip=219.142.118.237");
//设置下载用户的ip地址被限制在61.135.152.0/24网段，并在UNIX时间1175135000后生效
//$o->setExtra("?ip=1175135000,61.135.152.");

$r = $o->getFileUrl("foo/bar/1.html",$result);
echo "With ip limit: " . $result."\n";

$o::$domain = "http://";
$r = $o->getFileUrl("foo/bar/1.html",$result);
echo "Use project as domain: " . $result."\n";
