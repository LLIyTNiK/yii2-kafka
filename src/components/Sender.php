<?php


namespace lliytnik\yii2\kafka\components;
use lliytnik\yii2\kafka\components\Kafka;

class Sender extends Kafka
{
    public $producer;
    public $topic;
    public $resendNum = 5;
	public $flushTimeout = 100;

    public function init(){
        parent::init();
        $this->producer = new \RdKafka\Producer($this->_conf);
        $this->producer->addBrokers($this->brokers);
    }

    public function send($message,$topicName=null){
        if($topicName){
            $this->topic = $this->producer->newTopic($this->topicName);
        }else{
            $this->topic = $this->producer->newTopic($this->topicName);
        }

        $this->topic->produce(RD_KAFKA_PARTITION_UA,0,json_encode([
            'body' => serialize($message),
            'properties' => [],
            'headers' => [],
        ]));
        $this->producer->poll(0);
        for ($flushRetries = 0; $flushRetries < $this->resendNum; $flushRetries++) {
            $result = $this->producer->flush($this->flushTimeout);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }
        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            return false;
        }
        return true;
    }
}