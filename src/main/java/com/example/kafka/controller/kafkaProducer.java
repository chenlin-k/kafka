package com.example.kafka.controller;

import java.util.Properties;  
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
import kafka.serializer.StringEncoder;  
  
  
  
  
public class kafkaProducer extends Thread{  
  
    private String topic;  
      
    public kafkaProducer(String topic){  
        super();  
        this.topic = topic;  
    }  
      
      
    @Override  
    public void run() {  
        Producer producer = createProducer();  
        int i=0;
        String json="{\"datasource\":\"hive\",\"urlsource\":\"baidushixin\",\"model\":{\"age\":\"55\",\"areaName\":\"江西\",\"cardNum\":\"36222219611****5311\",\"caseCode\":\"（2017）赣0983执2038号\",\"courtName\":\"高安市人民法院\",\"disruptTypeName\":\"有履行能力而拒不履行生效法律文书确定义务\",\"duty\":\"判决如下：一、由被告赵飞如、席奕平于本判决生效后五日内支付所欠原告中国银行股份有限公司高安支行的信用卡汽车专向分期付款业务欠款本金111025.13元（利息和滞纳金按约定计算至本判决书确定给付之日止）。二、原告中国银行股份有限公司高安支行对被告赵飞如、席奕平提供抵押的车牌号为赣C0K792的奥迪牌小型普通客车折价或者以拍卖、变卖该抵押物所得价款优先受偿。三、被告赵新如、赵腊云对上述欠款承担连带清偿责任。如果未按本判决指定的期间履行给付金钱义务，应当按照《中华人民共和国民事诉讼法》第二百五十三条之规定，加倍支付迟延履行期间的债务利息。案件受理费2579元，由被告赵飞如、席奕平负担。\",\"gistId\":\"（2017）赣0983民初168号\",\"gistUnit\":\"高安市人民法院\",\"id\":\"1803070358502930000\",\"iname\":\"赵新22\",\"lastmod\":\"Wed Mar 07 15:58:50 CST 2018\",\"partyTypeName\":\"\",\"performance\":\"全部未履行\",\"publishDate\":\"2017年12月25日\",\"regDate\":\"20170921\",\"sexy\":\"男性\",\"sitelink\":\"http://shixin.court.gov.cn/\",\"type\":\"失信被执行人名单\"}}";


        while(true){
            producer.send(new KeyedMessage<Integer, String>(topic,json));
            try {  
                TimeUnit.SECONDS.sleep(1);  
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
        }
    }  
  
    private Producer createProducer() {  
        Properties properties = new Properties();  
        properties.put("zookeeper.connect", "192.168.88.127:2181");//声明zk
        properties.put("serializer.class", StringEncoder.class.getName());  
        properties.put("metadata.broker.list", "192.168.88.127:9092,192.168.88.121:9092,192.168.88.126:9092");// 声明kafka broker
        return new Producer<Integer, String>(new ProducerConfig(properties));  
     }  
      
      
    public static void main(String[] args) {  
        new kafkaProducer("test").start();// 使用kafka集群中创建好的主题 test
    }
       
} 