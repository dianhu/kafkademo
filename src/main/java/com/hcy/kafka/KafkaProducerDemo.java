package com.hcy.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Time : 17-1-12 下午2:05
 * Author : hcy
 * Description : kafka生产者示例
 */
public class KafkaProducerDemo {

    public Properties props;
    public final static String TOPIC = "toptest";

    public KafkaProducerDemo(){
        props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        //props.put("bootstrap.servers","172.16.2.94:9092,172.16.2.95:9092,172.16.2.102:9092");
        //props.put("key.deserializer", StringDeserializer.class.getName());
        //props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    }

    private void sendMsg() throws Exception{
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int messageNo = 1;
        final int COUNT = 101;

        int messageCount = 0;
        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String msg = "Hello kafka message :" + key;
            producer.send(new ProducerRecord(TOPIC,key,msg));
            Thread.sleep(1000l);
            System.out.println(msg);
            messageNo ++;
            messageCount++;
        }

        System.out.println("Producer端一共产生了" + messageCount + "条消息！");

    }

    public static class ProducerThread extends Thread{
        @Override
        public void run() {
            Properties props = new Properties();
            //props.put("bootstrap.servers","localhost:9092,localhost:9093,localhost:9094");
            props.put("bootstrap.servers","192.168.3.3:9092");
            //props.put("key.deserializer", StringDeserializer.class.getName());
            //props.put("value.deserializer", StringDeserializer.class.getName());
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            Producer<String, String> producer = new KafkaProducer<String, String>(props);
            int messageNo = 1;
            final int COUNT = 10001;

            int messageCount = 0;
            while (messageNo < COUNT) {
                String key = String.valueOf(messageNo);
                String msg = "Hello kafka message :" + key;

                producer.send(new ProducerRecord(TOPIC,key,msg),new Callback(){
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        System.out.println("Producer have sent message(topci="+metadata.topic()+",partition="+metadata.partition()+",offset="+metadata.offset()+")");
                    }
                });
                try {
                    Thread.currentThread().sleep(1000l);
                }catch (Exception e){

                }
                //System.out.println(record.toString());
                messageNo ++;
                messageCount++;
            }

            System.out.println("Producer端一共产生了" + messageCount + "条消息！");
        }
    }

    public static void main(String[] args) throws Exception {

//        KafkaProducerDemo producer = new KafkaProducerDemo();
//        producer.sendMsg();
        ProducerThread producer1 = new ProducerThread();
        //ProducerThread producer2 = new ProducerThread();
        producer1.start();
        //producer2.start();
    }
}
