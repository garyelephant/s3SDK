<?php
require("../SinaStorageService.php");
require_once( "conf.php" );

//写一个测试文件
/*
$localfile = "1.html";
$content = <<<HTML
<html>
  <body>
    <p>hello world.</p>
  </body>
</html>
HTML;
file_put_contents($localfile, $content);
*/

$localfile = $_SERVER["argv"][1];

//准备请求需要的数据
//$file_content = file_get_contents($localfile);

$file_content = "";
//$file_handle = fopen( $localfile , "rb");
$file_handle = fopen( $localfile , "r");
while (!feof($file_handle)) {
    $file_content = $file_content.fread($file_handle,81);
}
fclose($file_handle);


$file_type = mime_content_type( $localfile );
$file_length = filesize($localfile);
$file_sha1 = sha1($file_content);

$o = SinaStorageService::getInstance($project, $accesskey, $secretkey);

//可自定义配置的CURLOPT
$o->setCURLOPTs(array(CURLOPT_VERBOSE=>1));
$o->setQueryStrings(array("sldkfj"=>123));

//设置使用验证方式
$o->setAuth(true);

var_dump($o->uploadFile("foo/bar/$localfile",$file_content, $file_length, $file_type, $result));
//var_dump($o->uploadFile("foo/bar/1.html",$file_content, $file_length, "text/html",$result));
echo $result;
