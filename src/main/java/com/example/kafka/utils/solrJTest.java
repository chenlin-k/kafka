package com.example.kafka.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

public class solrJTest {
    SolrClient solrclient;

    public solrJTest(String solrURL){

        this.solrclient=new HttpSolrClient(solrURL);
    }
     public void close() {
            try {
                solrclient.close();
            } catch (IOException e) {

                e.printStackTrace();
            }
        }





//-----------增加文档：addDucument()---------------
//    @Test
    public void addDucument() throws IOException{
        System.out.println("======================add doc ===================");

        Collection<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();

        for (int i = 8; i < 9; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", 7*100);
            doc.addField("name", "user"+i);
            doc.addField("price", "100");
            doc.addField("description", "新增文档"+i);

            docs.add(doc);
        }
        SolrInputDocument doc1 = new SolrInputDocument();
        doc1.addField("id", 9*100);
        doc1.addField("date_s", "user");
        doc1.addField("inm_s", "100");

        docs.add(doc1);
        try {
            UpdateResponse rsp = solrclient.add(docs);
            System.out.println("Add doc size" + docs.size() + " result:" + rsp.getStatus() + " Qtime:" + rsp.getQTime());

            UpdateResponse rspcommit = solrclient.commit();
            System.out.println("commit doc to index" + " result:" + rsp.getStatus() + " Qtime:" + rsp.getQTime());

        } catch (SolrServerException  e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
}



//-----------查询文档：queryDocuments()-----------
//    @Test
    public void queryDocuments(){
        SolrQuery params = new SolrQuery();

        System.out.println("======================query===================");

        params.set("q", "*:*");
        params.set("start", 0);
        params.set("rows", 20);
        params.set("sort", "id asc");

        try {
            QueryResponse rsp = solrclient.query(params);
            SolrDocumentList docs = rsp.getResults();
            System.out.println("查询内容:" + params);
            System.out.println("文档数量：" + docs.getNumFound());
            System.out.println("查询花费时间:" + rsp.getQTime());

            System.out.println("------query data:------");
            for (SolrDocument doc : docs) {
                // 多值查询
                @SuppressWarnings("unchecked")
                String id = (String) doc.getFieldValue("id");
                String name = (String) doc.getFieldValue("iname_s");
                String regDate_s = String.valueOf(doc.getFieldValue("regDate_s"));
                String duty_s = (String) doc.getFieldValue("duty_s");

                System.out.println("id:"+id+"\t name:" + name + "\t duty_s:"+duty_s+"\t regDate_s:"+regDate_s );
            }
            System.out.println("-----------------------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




//-----------删除文档BY ID：deleteById()-----------
//    @Test
    public void deleteById(String id) {
        System.out.println("======================deleteById ===================");

        try {
            UpdateResponse rsp = solrclient.deleteById(id);
            solrclient.commit();
            System.out.println("delete id:" + id + " result:" + rsp.getStatus() + " Qtime:" + rsp.getQTime());
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }
    }



//-----------删除文档BY QUERY：deleteByQuery()-----------
    public void deleteByQuery(String query) {
        System.out.println("======================deleteByQuery ===================");

        UpdateResponse rsp;

        try {
            UpdateRequest commit = new UpdateRequest();
            commit.deleteByQuery(query);
            commit.setCommitWithin(500);
            commit.process(solrclient);
            System.out.println("url:"+commit.getPath()+"\t xml:"+commit.getXML()+" method:"+commit.getMethod());
//            rsp = client.deleteByQuery(query);
//            client.commit();
//            System.out.println("delete query:" + queryCon + " result:" + rsp.getStatus() + " Qtime:" + rsp.getQTime());
        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

    }


//-----------更新文档：updateDocuments()-----------
 public void updateDocuments(int id,String fieldName, Object fieldValue) {
        System.out.println("======================updateField ===================");
        HashMap<String, Object> oper = new HashMap<String, Object>();
//        多值更新方法
//        List<String> mulitValues = new ArrayList<String>();
//        mulitValues.add(fieldName);
//        mulitValues.add((String)fieldValue);
        oper.put("set", fieldValue);

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField(fieldName, oper);
        try {
            UpdateResponse rsp = solrclient.add(doc);
            System.out.println("update doc id:" + id + " result:" + rsp.getStatus() + " Qtime:" + rsp.getQTime());
            UpdateResponse rspCommit = solrclient.commit();
            System.out.println("commit doc to index" + " result:" + rspCommit.getStatus() + " Qtime:" + rspCommit.getQTime());

        } catch (SolrServerException | IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String args[]) throws IOException {
        solrJTest test = new solrJTest("http://192.168.88.127:8983/solr/collectionName");

        //添加文档
//         test.addDucument();

        // 删除文档
      //test.deleteById("200");
//        test.deleteByQuery("name:user3");

        //更新文档
//        test.updateDocuments(400, "name", "user新");

        // 查询文档
       test.addDucument();
        test.close();
    }
}