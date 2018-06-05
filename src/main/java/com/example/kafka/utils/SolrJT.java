package com.example.kafka.utils;


import java.io.IOException;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;


public class SolrJT {
    //指定solr服务器的地址
    private final static String SOLR_URL = "http://192.168.88.127:8983/solr/";

    private final static Logger log=Logger.getLogger(SolrJT.class);

    SolrClient solrclient;
    /**
     * 创建SolrServer对象
     *
     * 该对象有两个可以使用，都是线程安全的
     * 1、CommonsHttpSolrServer：启动web服务器使用的，通过http请求的
     * 2、 EmbeddedSolrServer：内嵌式的，导入solr的jar包就可以使用了
     * 3、solr 4.0之后好像添加了不少东西，其中CommonsHttpSolrServer这个类改名为HttpSolrClient
     *
     * @return
     */

    public SolrJT(){

        this.solrclient=new HttpSolrClient(SOLR_URL+"collectionName");
    }
    public void close() {
        try {
            solrclient.close();
        } catch (IOException e) {
            log.error("关闭solrclient异常："+e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 往索引库添加文档
     * @throws IOException
     * @throws SolrServerException
     */
    public void addDoc(SolrInputDocument document) throws SolrServerException, IOException{
        //构建document
        /*
        SolrInputDocument document=new SolrInputDocument();
        document.addField(?,?);
         */
        //获得一个solr服务端的请求，去提交  ,选择具体的某一个solr core
        HttpSolrClient solr = new HttpSolrClient(SOLR_URL + "collectionName");
        solr.add(document);
        solr.commit();
        solr.close();
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println("success!");
    }


    /**
     * 根据id从索引库删除文档
     */
    public void deleteDocumentById() throws Exception {
        //选择具体的某一个solr core
        HttpSolrClient server = new HttpSolrClient(SOLR_URL+"collectionName");
        //删除文档
        server.deleteById("8");
        //删除所有的索引
        //solr.deleteByQuery("*:*");
        //提交修改
        server.commit();
        server.close();
    }

    /**
     * 查询
     * @throws Exception
     */
    public String queryDocuments(int start,int rows){
        SolrQuery params = new SolrQuery();

        System.out.println("======================query===================");

        params.set("q", "*:*");
        params.set("start", start);
        params.set("rows", rows);
        params.set("sort", "id asc");
        JSONArray jsonArray=new JSONArray();
        JSONObject jsonObject=new JSONObject();
        try {
            QueryResponse rsp = solrclient.query(params);
            SolrDocumentList docs = rsp.getResults();
            jsonObject.put("num",docs.getNumFound());
            System.out.println("------query data:------");
            for (SolrDocument doc : docs) {
                JSONObject json=new JSONObject();
                Set<String>keys=doc.keySet();
                Iterator iterators= keys.iterator();
                while (iterators.hasNext()){
                    String key= (String) iterators.next();
                    json.put(key,doc.getFieldValue(key));
                }
                jsonArray.add(json);
            }
            jsonObject.put("jsonArray",jsonArray);
        } catch (Exception e) {
            log.error("查询solr异常："+e.getMessage());
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    public static void main(String[] args) throws Exception {
//        String json = "{\"id\":\"19872\",\"datasource\":\"hive\",\"urlsource\":\"baidushixin\",\"model\":{\"age\":\"55\",\"areaName\":\"江西\",\"cardNum\":\"36222219611****5311\",\"caseCode\":\"（2017）赣0983执2038号\",\"courtName\":\"高安市人民法院\",\"disruptTypeName\":\"有履行能力而拒不履行生效法律文书确定义务\",\"duty\":\"判决如下：一、由被告赵飞如、席奕平于本判决生效后五日内支付所欠原告中国银行股份有限公司高安支行的信用卡汽车专向分期付款业务欠款本金111025.13元（利息和滞纳金按约定计算至本判决书确定给付之日止）。二、原告中国银行股份有限公司高安支行对被告赵飞如、席奕平提供抵押的车牌号为赣C0K792的奥迪牌小型普通客车折价或者以拍卖、变卖该抵押物所得价款优先受偿。三、被告赵新如、赵腊云对上述欠款承担连带清偿责任。如果未按本判决指定的期间履行给付金钱义务，应当按照《中华人民共和国民事诉讼法》第二百五十三条之规定，加倍支付迟延履行期间的债务利息。案件受理费2579元，由被告赵飞如、席奕平负担。\",\"gistId\":\"（2017）赣0983民初168号\",\"gistUnit\":\"高安市人民法院\",\"id\":\"1803070358502930000\",\"iname\":\"赵新22\",\"lastmod\":\"Wed Mar 07 15:58:50 CST 2018\",\"partyTypeName\":\"\",\"performance\":\"全部未履行\",\"publishDate\":\"2017年12月25日\",\"regDate\":\"20170921\",\"sexy\":\"男性\",\"sitelink\":\"http://shixin.court.gov.cn/\",\"type\":\"失信被执行人名单\"}}";
//        JSONObject jsonObject = JSON.parseObject(json);
//        Set<String> keys = jsonObject.keySet();
//        Iterator iterators = keys.iterator();
//        //开始solr服务
//        //创建文档并将json数据添加进去
//        SolrInputDocument documents = new SolrInputDocument();
//        while (iterators.hasNext()) {
//            String key = (String) iterators.next();
//            if (key.equals("id")) {
//                documents.addField(key, jsonObject.getString(key));
//            } else {
//                documents.addField(key + "_s", jsonObject.getString(key));
//            }
//        }
//        System.out.println("开始插入到solr...");
        SolrJT solr = new SolrJT();
//        solr.addDoc(documents);
        // 查询文档
        solr.queryDocuments(0,2);
        solr.close();
    }
}
