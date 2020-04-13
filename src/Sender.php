<?php


namespace lliytnik\yii2-kafka;


class Sender extends Kafka
{
    public $producer;
    public $topic;
    public $resendNum = 5;

    public function init(){
        parent::init();
        $this->producer = new RdKafka\Producer($this->_conf);
        $this->producer->addBrokers($this->brokers);
        $this->topic = $this->producer->newTopic($this->topicName);
    }

    public function send($message){
        $this->topic->produce(RD_KAFKA_PARTITION_UA,0,json_encode($message));
        $this->producer->poll(0);
        for ($flushRetries = 0; $flushRetries < $this->resendNum; $flushRetries++) {
            $result = $this->producer->flush(10000);
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