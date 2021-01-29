<?php


namespace lliytnik\yii2\kafka\components;
use lliytnik\yii2\kafka\components\Kafka;
use phpDocumentor\Reflection\Types\This;

class Sender extends Kafka
{
    public $producer;
    public $topic;
    public $resendNum = 5;
	public $flushTimeout = 100;
	public $failFolderPath = '';

    public function init(){
        parent::init();
        $this->producer = new \RdKafka\Producer($this->_conf);
        $this->producer->addBrokers($this->brokers);
    }

    public function eventSend($event){
        $this->topic = $this->producer->newTopic($this->topicName);
        $this->topic->produce(RD_KAFKA_PARTITION_UA,0,json_encode([
            'body' => serialize($event),
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
            $this->saveToFile($event,'',true);
        }
        return true;
    }

    public function dataSend($fields,$topicName)
    {
        $topic = $this->producer->newTopic($topicName);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($fields));
        $this->producer->poll(0);
        for ($flushRetries = 0; $flushRetries < $this->resendNum; $flushRetries++) {
            $result = $this->producer->flush($this->flushTimeout);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }
        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            $this->saveToFile($fields,$topicName,false);
        }
        return true;
    }

    public function saveToFile($attrs, $fileName,$is_event=true){
        $folder = \Yii::getAlias('@console').$this->failFolderPath;
        $fileName = $folder.time().'_'.($is_event?'event':'save_data_to').'_'.$fileName.rand(0,10000).'.csv';
        while(!$fHandle = fopen($fileName,'x')){
            $fileName = $folder.time().'_'.($is_event?'event':'save_data_to').'_'.$fileName.rand(0,10000).'.csv';
        }
        if($is_event){
            fputs($fHandle,json_encode($attrs));
        }else{
            fputcsv($fHandle,$attrs);
        }
        fclose($fHandle);
    }
}