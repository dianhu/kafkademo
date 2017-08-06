package com.hcy.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.util.*;

/**
 * Time : 17-8-6 下午10:05
 * Author : hcy
 * Description :
 */
public class ConsumerExactlyOnceDemo {
    public final static String TOPIC = "test1";
    public String group;
    public ConsumerExactlyOnceDemo(String group){
        this.group = group;
    }

    public static class ConsumerThread extends Thread{
        String group;
        public ConsumerThread(String group){
            this.group = group;
        }
        @Override
        public void run() {
            JdbcUtils jdbcUtils = new JdbcUtils();
            Connection conn = jdbcUtils.getConnection();
            Consumer<String, String> consumer = null;
            try{
                System.out.println("线程："+this.toString());
                Properties props = new Properties();
                //props.put("bootstrap.servers","localhost:9092,localhost:9093,localhost:9094");
                props.put("bootstrap.servers","localhost:9092");
                props.put("key.deserializer", StringDeserializer.class.getName());
                props.put("value.deserializer", StringDeserializer.class.getName());
                //props.put("max.poll.records", "10");
                props.put("group.id", this.group);
                //props.put("auto.offset.reset", "latest");//当前offset不存在，即消费者从没有提交过offset，从最近的消息消费
                props.put("enable.auto.commit","false");
                consumer = new KafkaConsumer<String, String>(props);


                while (true) {
                    //从mysql中查找对应的offset
                    String sql = "select * from CONSUMER_OFFSETS where congroup = ? and contopic = ?";
                    List<Object> params = new ArrayList<Object>();
                    params.add("GroupB");
                    params.add("toptest");
                    List<ConsumerOffsets> offsets = jdbcUtils.findMoreRefResult(sql,params,ConsumerOffsets.class);
                    if(offsets.size()>0){
                        for(ConsumerOffsets offset :offsets){
                            TopicPartition tp = new TopicPartition(offset.getContopic(),offset.getConpartition());
                            consumer.unsubscribe();
                            consumer.assign(Collections.singletonList(tp));
                            consumer.seek(tp,offset.getConoffset());
                        }
                    }else {//如果没找到则直接订阅
                        consumer.subscribe(Arrays.asList("toptest"));
                    }


                    ConsumerRecords<String, String> records = consumer.poll(100);
                    //Thread.currentThread().sleep(1000l);
                    for (ConsumerRecord<String, String> record : records) {

                        System.out.println("消费消息开始：record="+record.value());

                        conn.setAutoCommit(Boolean.FALSE);

                        //业务插入
                        String sql1 = "insert into BUSINESS ( msgkey , message ) values (?,?)";
                        List<Object> params1 = new ArrayList<Object>();
                        params1.add(record.key());
                        params1.add(record.topic()+":"+record.partition()+":"+record.offset()+":"+record.value());
                        jdbcUtils.updateByPreparedStatement(sql1,params1);

                        //存储offset
                        String sql2;
                        List<Object> params2;
                        List<ConsumerOffsets> offsets1 = jdbcUtils.findMoreRefResult(sql,params,ConsumerOffsets.class);
                        if(offsets1.size()==0){
                            sql2 = "insert into CONSUMER_OFFSETS (congroup,contopic,conpartition,conoffset) values (?,?,?,?)";
                            params2 = new ArrayList<Object>();
                            params2.add(group);
                            params2.add(record.topic());
                            params2.add(record.partition());
                            params2.add(record.offset());
                        }else{
                            sql2 = "update CONSUMER_OFFSETS set conoffset = ? where congroup = ? and contopic=? and conpartition=?";
                            params2 = new ArrayList<Object>();
                            params2.add(record.offset());
                            params2.add(group);
                            params2.add(record.topic());
                            params2.add(record.partition());
                        }

                        jdbcUtils.updateByPreparedStatement(sql2,params2);
                        //提交事务
                        conn.commit();
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                consumer.close();
                jdbcUtils.releaseConn();
            }

        }
    }

    public static class ConsumerOffsets{
        private String congroup;
        private String contopic;
        private int conpartition;
        private long conoffset;

        public String getCongroup() {
            return congroup;
        }

        public void setCongroup(String congroup) {
            this.congroup = congroup;
        }

        public String getContopic() {
            return contopic;
        }

        public void setContopic(String contopic) {
            this.contopic = contopic;
        }

        public int getConpartition() {
            return conpartition;
        }

        public void setConpartition(int conpartition) {
            this.conpartition = conpartition;
        }

        public long getConoffset() {
            return conoffset;
        }

        public void setConoffset(long conoffset) {
            this.conoffset = conoffset;
        }
    }

    public static void main(String[] args) throws Exception {

        new ConsumerExactlyOnceDemo.ConsumerThread("GroupB").start();
        //new ConsumerExactlyOnceDemo.ConsumerThread("GroupB").start();
        //new ConsumerThread("GroupA").start();
    }
}
