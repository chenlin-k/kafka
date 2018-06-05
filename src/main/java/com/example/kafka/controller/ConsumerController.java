package com.example.kafka.controller;


import com.alibaba.fastjson.JSONArray;
import com.example.kafka.utils.SolrJT;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@EnableAutoConfiguration
public class ConsumerController {

    @RequestMapping("/querySolr")
    public String in(int start,int rows) {
        SolrJT solrJT= new SolrJT();
        String results=solrJT.queryDocuments(start,rows);
        return results;
    }
}
