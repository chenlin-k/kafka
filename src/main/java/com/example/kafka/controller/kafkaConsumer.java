package com.example.kafka.controller;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.kafka.hdfs.HdfsUtils;
import com.example.kafka.model.SpliderError;
import com.example.kafka.utils.DBManager;
import com.example.kafka.utils.JDBCConnetct;
import com.example.kafka.utils.MysqlJT;
import com.example.kafka.utils.SolrJT;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.StringUtils;


/** 
 * 接收数据
 *
 * json数据格式：
 "{"datasource":"solr", "urlsource":"baidushixin","model":"~"}
 * datasource表示数据库类型
 * urlsource表示数据来源为hive或者mysql时，这个参数被用于创建表。
 * model表示抓取的数据
 *
 * @author chenlink
 * 
 */

public class kafkaConsumer extends Thread{
  
    private String topic;

    private static final Logger logger = Logger.getLogger(kafkaConsumer.class);

    public kafkaConsumer(){
        super();
    }
      
      
    @Override  
    public void run() {
        Connection con = null;
        try {
            con = DBManager.getInstanse().getConnection(DBManager.DB_ALIAS_DEFAULT);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        JDBCConnetct jd=new JDBCConnetct();
        SpliderError serror=new SpliderError();
        try {
            this.topic=jd.querySpliderTopic(con);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("查询数据库中主题参数出错！");
        }
        ConsumerConnector consumer = createConsumer();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();  
        topicCountMap.put(topic, 1); // 一次从主题中获取一个数据  
         Map<String, List<KafkaStream<byte[], byte[]>>>  messageStreams = consumer.createMessageStreams(topicCountMap);  
         KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);// 获取每次接收到的这个数据  
         ConsumerIterator<byte[], byte[]> iterator =  stream.iterator();
         int i=10;
        String datasource =null;
        while(iterator.hasNext()){
             String message = new String(iterator.next().message());  
             System.out.println("接收到: " + message);
             Map<String, Object> map = null;
             try {
                 map = (Map<String,Object>) JSON.parse(message);
             } catch (Exception e) {
                 logger.error("解析参数出错！");
                 serror.setDetail(message);
                 serror.setTopic(topic);
                 serror.setCreatetime(new Date());
                 serror.setErrorresion(e.getLocalizedMessage()+"解析参数出错！");
                 try {
                     jd.saveSpliderError(con,serror);
                 } catch (SQLException e1) {
                    logger.error("保存splider_error异常！");
                 }
                 continue ;
             }
             datasource = map.get("datasource") == null ? "" : map.get("datasource").toString();
             String urlsource=map.get("urlsource") == null ? "" : map.get("urlsource").toString();
             String model=map.get("model") == null ? "" : map.get("model").toString();
             if(!org.springframework.util.StringUtils.hasText(datasource)){
                 logger.error("传入数据的datasource数据库源参数为空！");
                 serror.setDetail(message);
                 serror.setTopic(topic);
                 serror.setUrl(urlsource);
                 serror.setCreatetime(new Date());
                 serror.setErrorresion("传入数据的datasource数据库源参数为空！");
                 try {
                     jd.saveSpliderError(con,serror);
                 } catch (SQLException e1) {
                     logger.error("保存splider_error异常！"+ e1.getMessage());
                 }
                 continue ;
             }
             if(!org.springframework.util.StringUtils.hasText(urlsource)){
                 logger.error("传入数据的网站数据源参数为空！");
                 serror.setDetail(message);
                 serror.setTopic(topic);
                 serror.setUrl(urlsource);
                 serror.setCreatetime(new Date());
                 serror.setErrorresion("传入数据的网站数据源参数为空！");
                 try {
                     jd.saveSpliderError(con,serror);
                 } catch (SQLException e1) {
                     logger.error("保存splider_error异常！"+ e1.getMessage());
                 }
                 continue ;
             }
             if(!org.springframework.util.StringUtils.hasText(model)){
                 logger.error("传入数据中的model爬取数据参数为空！");
                 serror.setDetail(message);
                 serror.setTopic(topic);
                 serror.setUrl(urlsource);
                 serror.setCreatetime(new Date());
                 serror.setErrorresion("传入数据中的model爬取数据参数为空！");
                 try {
                     jd.saveSpliderError(con,serror);
                 } catch (SQLException e1) {
                     logger.error("保存splider_error异常！"+ e1.getMessage());
                 }
                 continue ;
             }
//             Map<String,String> stringMap =new HashMap<>();
             JSONObject jsonObject = JSON.parseObject(model);
             Set<String> keys = jsonObject.keySet();
             Iterator iterators = keys.iterator();
             //开始solr服务
             if(datasource.equals("solr")) {
                 //创建文档并将json数据添加进去
                 SolrInputDocument documents = new SolrInputDocument();
                 while(iterators.hasNext()){
                     String key = (String) iterators.next();
                     if(key.equals("id")){
                         documents.addField(key,jsonObject.getString(key));
                     }else{
                         documents.addField(key+"_s",jsonObject.getString(key));
                     }

                 }
                 System.out.println("开始插入到solr...");
                 SolrJT solr = new SolrJT();
                 try {
                     solr.addDoc(documents);
                 } catch (Exception e) {
                     serror.setDetail(message);
                     serror.setTopic(topic);
                     serror.setUrl(urlsource);
                     serror.setCreatetime(new Date());
                     serror.setErrorresion("插入到solr出现异常！");
                     try {
                         jd.saveSpliderError(con,serror);
                     } catch (SQLException e1) {
                         logger.error("保存splider_error异常！"+ e1.getMessage());
                     }
                     continue ;
                 }
                 System.out.println("插入solr结束！");
             }
             //开始hive服务
             //注：hive的字符串中的key最好不要以_为开头。hive建表时不支持以_开头的列名
             if(datasource.equals("hive")) {
                 if(StringUtils.isEmpty(urlsource)){
                     serror.setDetail(message);
                     serror.setTopic(topic);
                     serror.setUrl(urlsource);
                     serror.setCreatetime(new Date());
                     serror.setTablename(urlsource);
                     serror.setErrorresion("插入到hive异常！表名为空");
                     try {
                         jd.saveSpliderError(con,serror);
                     } catch (SQLException e1) {
                         logger.error("保存splider_error异常！"+ e1.getMessage());
                     }
                     continue ;
                 }else{
                     HdfsUtils hd = new HdfsUtils();
                     hd.WriteTxt(jsonObject,urlsource);
                 }
             }
             //开始mysql服务
             if(datasource.equals("mysql")) {
                 if(StringUtils.isEmpty(urlsource)){
                     serror.setDetail(message);
                     serror.setTopic(topic);
                     serror.setUrl(urlsource);
                     serror.setCreatetime(new Date());
                     serror.setTablename(urlsource);
                     serror.setErrorresion("插入到mysql异常！表名为空");
                     try {
                         jd.saveSpliderError(con,serror);
                     } catch (SQLException e1) {
                         logger.error("保存splider_error异常！"+ e1.getMessage());
                     }
                     continue ;
                 }else{
                     MysqlJT mj=new MysqlJT();
                     mj.createTable(urlsource,"mysql",jsonObject);
                 }
             }
         }
    }

    private ConsumerConnector createConsumer() {
        Properties properties = new Properties();  
        properties.put("zookeeper.connect", "192.168.88.127:2181");//声明zk
        properties.put("group.id", "group1");// 必须要使用别的组名称， 如果生产者和消费者都在同一组，则不能访问同一组内的topic数据  
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));  
     }


    public static void main(String[] args) {
        new kafkaConsumer().start();// 使用kafka集群中创建好的主题 test
    }
}