<?php
require_once('../SinaStorageService.php');

class getInstanceTest extends PHPUnit_Framework_TestCase
{
    protected $obj;

    /**
     * @covers SinaStorageService::getInstance
     * @group getInstance
     * @dataProvider getInstanceData
     */
    public function testGetInstance($conf1)
    {
        $this->obj = SinaStorageService::getInstance(
            $conf1['project']
        );
        $this->obj->getFile("foo/bar/1.html", $result);
        $httpCode = $this->obj->result_info['http_code'];
        $this->assertEquals( 200, $httpCode);
        $this->obj->result_info = null;
        $firstSerConf1 = serialize($this->obj);

        $this->obj = SinaStorageService::getInstance(
            $conf1['project'], $conf1['accesskey'], $conf1['secretkey']
        );
        $this->obj->getFile("foo/bar/1.html", $result);
        $httpCode = $this->obj->result_info['http_code'];
        $this->assertEquals( 200, $httpCode);
        $this->obj->result_info = null;
        $secondSerConf1 = serialize($this->obj);
        $this->assertNotEquals($secondSerConf1, $firstSerConf1);

        $this->obj->setExtra("?log");
        $thirdSerConf1 = serialize($this->obj);
        $this->obj = SinaStorageService::getInstance(
            $conf1['project'], $conf1['accesskey'], $conf1['secretkey']
        );
        $forthSerConf1 = serialize($this->obj);
        $this->assertEquals($thirdSerConf1, $forthSerConf1);

        $this->obj = SinaStorageService::getInstance(
            $conf1['project'], $conf1['accesskey'], $conf1['secretkey'], true
        );
        $fifthSerConf1 = serialize($this->obj);
        $this->assertNotEquals($thirdSerConf1, $fifthSerConf1);

        $this->obj = SinaStorageService::getInstance(
            $conf1['project'], $conf1['accesskey'], $conf1['secretkey'], true
        );
        $this->obj->setAuth(true);
        $this->obj->getFile("foo/bar/1.html", $result);
        $httpCode = $this->obj->result_info['http_code'];

        $this->assertEquals( 200, $httpCode);
    }

    public function getInstanceData()
    {
        $conf1 = array(
            'project'=>'sandbox',
            'accesskey'=>'SYS0000000000SANDBOX',
            'secretkey'=>'1111111111111111111111111111111111111111'
        );
        return array(
            array($conf1)
        );
    }
}
?>
