<?php
require("../SinaStorageService.php");

$project = "sandbox";
$accesskey = "SYS0000000000SANDBOX";
$secretkey = "1111111111111111111111111111111111111111";

//写一个文件用来测试上传等操作
$localfile = "1.html";
$content = <<<HTML
<html>
  <body>
    <p>hello world.</p>
  </body>
</html>
HTML;
file_put_contents($localfile, $content);

//准备请求需要的数据
$file_content = file_get_contents($localfile);
$file_length = filesize($localfile);
$file_sha1 = sha1($file_content);

$o = SinaStorageService::getInstance($project, $accesskey, $secretkey);

//有些时候可能需要一个全新的对象，可以增加第4个参数用来获取
//$o = SinaStorageService::getInstance($project, $accesskey, $secretkey, true);

//以下3个方法都是为了便于扩展，允许使用者构造任意的HTTP请求
//可自定义cURL请求的CURLOPT
$o->setCURLOPTs(array(CURLOPT_VERBOSE=>1));

//可自定义请求的header
//fortest: 1
$o->setRequestHeaders(array("fortest"=>1));

//可自定义请求的querystring
// &foo=bar
$o->setQueryStrings(array("foo"=>"bar"));

//设置是否使用验证方式  需提供accesskey,secretkey
$o->setAuth(true);

//设置一些特殊行为 具体请参看文档
//$o->setExtra("?acl");
//设置IP访问权限,获取一个只允许以下IP访问的url
//$o->setExtra("?ip=61.135.152.194");
//下载用户的ip地址被限制在61.135.152.0/24网段，并在UNIX时间1175135000后生效
$o->setQueryStrings(array("ip"=>"1175135000,61.135.152."));
$o->getFileUrl("1123.html",$result);
echo $result;
exit;
//设置请求过期时间,默认2*3600秒
//$o->setExpires(time()+7200);





//获取文件例子
echo "========getfile=========\n";
//用$result获取结果内容，  getFile方法只返回成败与否
var_dump($o->getFile("1123.html",$result));
echo $result;
echo "========getfile=========\n";
echo "\n\n";
