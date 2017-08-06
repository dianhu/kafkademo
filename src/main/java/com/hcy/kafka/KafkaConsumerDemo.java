package com.hcy.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Time : 17-1-12 下午3:16
 * Author : hcy
 * Description :
 */
public class KafkaConsumerDemo {
    public final static String TOPIC = "test1";
    public String group;
    public KafkaConsumerDemo(String group){
        this.group = group;
    }

//    private void handleMsg(){
//        Properties props = new Properties();
//        props.put("bootstrap.servers","localhost:9092");
//        props.put("key.deserializer", StringDeserializer.class.getName());
//        props.put("value.deserializer", StringDeserializer.class.getName());
//        props.put("max.poll.records", "10");
//        props.put("group.id", this.group);
//        props.put("enable.auto.commit","false");
//        Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
//        consumer.subscribe(Arrays.asList("test"));
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(100);
//            for (ConsumerRecord<String, String> record : records)
//                //　正常这里应该使用线程池处理，不应该在这里处理
//                System.out.printf("consumer:"+this.group+"-----offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value()+"\n");
//
//        }
//
//    }
    public static class ConsumerThread extends Thread{
        String group;
        public ConsumerThread(String group){
            this.group = group;
        }
        @Override
        public void run() {
            try{
                System.out.println("线程："+this.toString());
                Properties props = new Properties();
                //props.put("bootstrap.servers","localhost:9092,localhost:9093,localhost:9094");
                props.put("bootstrap.servers","172.16.2.94:9092,172.16.2.95:9092,172.16.2.102:9092");
                props.put("key.deserializer", StringDeserializer.class.getName());
                props.put("value.deserializer", StringDeserializer.class.getName());
                props.put("max.poll.records", "10");
                props.put("group.id", this.group);
                props.put("auto.offset.reset", "latest");//当前offset不存在，即消费者从没有提交过offset，从最近的消息消费
                props.put("enable.auto.commit","true");
                Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
                consumer.subscribe(Arrays.asList("hcy"));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    //Thread.currentThread().sleep(1000l);
                    for (ConsumerRecord<String, String> record : records)
                        //　正常这里应该使用线程池处理，不应该在这里处理
                        System.out.printf("consumer:"+this.toString()+this.group+"-----offset = %d, partition = %d, key = %s, value = %s", record.offset(), record.partition(),record.key(), record.value()+"\n");

                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    public static void main(String[] args) throws Exception {

        new ConsumerThread("GroupB").start();
        new ConsumerThread("GroupB").start();
        //new ConsumerThread("GroupA").start();
    }
}
