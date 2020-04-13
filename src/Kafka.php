<?php


namespace lliytnik\kafka;


use yii\base\Component;

class Kafka extends Component
{
    protected $_conf;
    public $params;
    public $kafka;

    /* producer
        ['group.id','NAME']
    */
    /* consumer
    [
        'group.id','NAME']
        'auto.commit.interval.ms'=> 100,
        'offset.store.method'=> 'broker',
        'auto.offset.reset'=> 'smallest',
    ];*/

    public $topicConf = [];
    public $brokers = '127.0.0.1';
    public $topicName = 'test';
    public $logLevel = LOG_ERR;
    public $groupID = 'myGroup';

    public function init(){
        parent::init();
        $this->_conf = new RdKafka\Conf();
        $this->_conf->set('log_level', (string) $this->logLevel);
        foreach ($this->params as $configName => $configValue){
            $this->_conf->set($configName, $configValue);
        }

    }

}