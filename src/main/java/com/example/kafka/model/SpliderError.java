package com.example.kafka.model;


import java.util.Date;

public class SpliderError {

    private String datasource;
    private String topic;
    private String detail;
    private String url;
    private String errorresion;
    private Date createtime;
    private String tablename;

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public Date getCreatetime() {
        return createtime;
    }

    public void setCreatetime(Date createtime) {
        this.createtime = createtime;
    }

    public String getDatasource() {
        return datasource;
    }

    public void setDatasource(String datasource) {
        this.datasource = datasource;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDetail() {
        return detail;
    }

    public void setDetail(String detail) {
        this.detail = detail;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getErrorresion() {
        return errorresion;
    }

    public void setErrorresion(String errorresion) {
        this.errorresion = errorresion;
    }
}
