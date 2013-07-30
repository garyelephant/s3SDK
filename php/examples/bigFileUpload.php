<?php
require ("../SinaStorageService.php");
require_once ("conf.php");

if (!function_exists('mime_content_type')) {
	function mime_content_type($f) {
		return trim(exec('file-bi' . escapeshellarg($f)));
	}
}

$o = SinaStorageService::getInstance($project, $accesskey, $secretkey);

$big_file_name = isset ($_SERVER["argv"][1]) ? $_SERVER["argv"][1] : "conf.php";
$big_file_split_str = isset ($_SERVER["argv"][2]) ? $_SERVER["argv"][2] : "1K";

$_unit = substr($big_file_split_str, -1, 1);
if ($_unit == "G" || $_unit == "g") {
	$big_file_split = substr($big_file_split_str, 0, (strlen($big_file_split_str) - 1));
	$big_file_split = $big_file_split * 1024 * 1024 * 1024;
}
elseif ($_unit == "M" || $_unit == "m") {
	$big_file_split = substr($big_file_split_str, 0, (strlen($big_file_split_str) - 1));
	$big_file_split = $big_file_split * 1024 * 1024;
}
elseif ($_unit == "K" || $_unit == "k") {
	$big_file_split = substr($big_file_split_str, 0, (strlen($big_file_split_str) - 1));
	$big_file_split = $big_file_split * 1024;
} else {
	$big_file_split = $big_file_split_str;
}

$dest_name = "test/$big_file_name";

/*
 * split big file
 * you can save the parts into files not memory
 */
$file_length = filesize($big_file_name);
$file_contype = mime_content_type($big_file_name);

$info = array();

$_start = 0;
$_part = 1;
while ($_start < $file_length) {

	$_content = file_get_contents($big_file_name, NULL, NULL, $_start, $big_file_split);

	$info["".$_part] = array (
						"partNumber" => $_part,
						"md5" => md5($_content),
						"content" => $_content,
						"size" => strlen($_content),
						"type" => "application/octet-stream",
					);

	$_start += $big_file_split;
	$_part += 1;
}

/* Get uplaodId
* getuploadId
*/
$o->purgeParams();
$o->setCURLOPTs(array (
	CURLOPT_VERBOSE => 0,
	CURLOPT_TIMEOUT => 20,
));
$o->setAuth(true);

$result = null;

if($o->getuploadId($dest_name, $file_contype, $result)) {
	try{
		$arr_xml = (array)simplexml_load_string($result);
		$uploadId = $arr_xml["UploadId"];
		echo "uploadid : " . $uploadId . "\n";
	} catch (Exception $e){
		print_r($e);
		echo "get uploadid error\n";
		exit;
	}
} else {
	echo "get uploadid error\n";
	exit;
}

/*
* uploadPart
*/
$merge_content = '<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUpload>';
foreach ($info as $k => $one) {
	$o->purgeParams();
	$o->setCURLOPTs(array (
		CURLOPT_VERBOSE => 0,
		CURLOPT_TIMEOUT => 60,
	));
	$o->setAuth(true);

	$merge_content .= "<Part><PartNumber>" . $one["partNumber"] . "</PartNumber><ETag>" . $one["md5"] . "</ETag></Part>";

	try{
		if ($o->uploadPart($dest_name, $one["content"], $one["size"], $one["type"], $one["partNumber"], $uploadId, $result)) {
			echo "upload part " . $one["partNumber"] . " OK\n";
		}
	} catch (SinaServiceException $e){
		continue;
	}
}
$merge_content .= "</CompleteMultipartUpload>";

$is_all_ok = false;
$round = 1;
while(!$is_all_ok && $round <= 5 ){
	$round += 1;

	if($o->getuploadParts($dest_name, $uploadId, $result)) {
		try{
			$arr_xml = (array)simplexml_load_string($result);

			$uploaded = array();
			if(is_array($arr_xml["Part"])){
				$parts = (array)$arr_xml["Part"];
				foreach( $parts as $v ){
					$v = (array)$v;
					$num = "".$v["PartNumber"];
					$uploaded[$num] = $v["ETag"];
				}
			} else {
				$parts = (array)$arr_xml["Part"];
				$num = "".$parts["PartNumber"];
				$uploaded[$num] = $parts["ETag"];
			}

			$unupload = array();
			foreach( $info as $k => $v ){
				if(!array_key_exists($k, $uploaded)
					|| $uploaded[$k] != $v["md5"] ){
					$unupload[$k] = $info[$k];
				}
			}

			if( count($unupload) == 0 ){
				$is_all_ok = true;
				break;
			}

			foreach ($unupload as $k => $one) {
				$o->purgeParams();
				$o->setCURLOPTs(array (
					CURLOPT_VERBOSE => 0,
					CURLOPT_TIMEOUT => 60,
				));
				$o->setAuth(true);

				try{
					if ($o->uploadPart($dest_name, $one["content"], $one["size"], $one["type"], $one["partNumber"], $uploadId, $result)) {
						echo "upload part " . $one["partNumber"] . " OK\n";
					}
				} catch (SinaServiceException $e){
					continue;
				}
			}
		} catch (Exception $e){
			continue;
		}
	} else {
		continue;
	}
}


/*
* mergePart
*/
if(!$is_all_ok){
	echo "$dest_name uploads faild, because parts are not complete.\n";
	exit;
}

$o->purgeParams();
$o->setCURLOPTs(array (
	CURLOPT_VERBOSE => 0,
	CURLOPT_TIMEOUT => 120,
));
$o->setAuth(true);

$result = null;
try{
	if ($o->mergeParts($dest_name, $merge_content, strlen($merge_content), "application/xml", $uploadId, $result)) {
		echo "merge $dest_name OK\n";
	}
} catch (SinaServiceException $e){
	print_r($e);
	print_r($result);
}

?>

