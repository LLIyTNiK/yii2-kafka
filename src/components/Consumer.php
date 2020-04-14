<?php


namespace lliytnik\kafka\components;


class Consumer extends Kafka
{
    public $consumer;
    public $topic;
    public $offsetResetType = RD_KAFKA_OFFSET_STORED;
    public $consumeMessageNumber = 1;
    public $sleep = 0;
    public $messageCallBack;

    public function init(){
        parent::init();
        $this->consumer = new \RdKafka\Consumer($this->_conf);
        $this->consumer->addBrokers($this->brokers);
        $topicConfig = new \RdKafka\TopicConf();
        foreach ($this->topicConf as $confName=>$confValue){
            $topicConfig->set($confName,$confValue);
        }
        $this->topic = $this->consumer->newTopic($this->topicName,$topicConfig);
    }

    public function consume(){
        $this->topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
        if($this->consumeMessageNumber==1){
            $this->consumeOne();
        }else{
            $this->consumeBatch();
        }
    }

    public function consumeOne(){
        while (true) {
            $message = $topic->consume (0, 1000);
            if(!empty($message)){
                $this->processMessage($message);
            }
        }
    }

    public function consumeBatch(){
        while (true) {
            $messages = $this->topic->consumeBatch (0, 1000,$this->consumeMessageNumber);
            if(!empty($messages)){
                $this->processMessages($messages);
                $message = $messages[0];
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        var_dump($message);
                        break;
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        echo "No more messages; will wait for more\n";
                        break;
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        echo "Timed out\n";
                        break;
                    default:
                        throw new \Exception($message->errstr(), $message->err);
                        break;
                }
            }

        }
    }

    public function processMessage($message){
        if($this->messageCallBack){
            ($this->messageCallBack)($message);
//            switch ($message->err) {
//                case RD_KAFKA_RESP_ERR_NO_ERROR:
//                    var_dump($message);
//                    break;
//                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
//                    echo "No more messages; will wait for more\n";
//                    break;
//                case RD_KAFKA_RESP_ERR__TIMED_OUT:
//                    echo "Timed out\n";
//                    break;
//                default:
//                    throw new \Exception($message->errstr(), $message->err);
//                    break;
//            }
        }
        else {
            throw new \Exception("you need to Create new class");
        }
    }

    public function processMessages($messages){
        if($this->messageCallBack){
            ($this->messageCallBack)($messages);
        }else {
            throw new \Exception("you need to Create new class");
        }
    }
}