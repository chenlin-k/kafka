package com.example.kafka.utils;


import com.example.kafka.model.SpliderError;
import com.example.kafka.model.SpliderTopic;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

/*
    jdbc连接类

    chenlink
 */


public class JDBCConnetct {

    private static final Logger logger = Logger.getLogger(JDBCConnetct.class);

    private String query_splider_topic="SELECT topic FROM splider_topic where id=1";

    private String query_Table_Name="SELECT COUNT(*) counts FROM splider_tablename where tablename=? and datasource=?";

    private String insert_Table_Name="insert into splider_tablename (tablename,createtime,datasource) values (?,?,?)";

    private String insert_splider_error="insert into splider_error (datasource,topic,detail,url,errorresion,createtime,tablename) values(?,?,?,?,?,?,?)";


    public String querySpliderTopic(Connection conn) throws SQLException{
        PreparedStatement ps = null;
        ResultSet rs = null;
        String topic=null;
        try {
            ps = conn.prepareStatement(query_splider_topic);
            rs = ps.executeQuery();
            while(rs.next()){
                topic=rs.getString("topic");
            }
        }catch (SQLException e) {
            logger.error("查询主题数据异常！\n"+e);
            throw e;
        }finally{
            DBManager.close(rs,ps,null);
        }
        return topic;
    }
    public void saveSpliderError(Connection conn,SpliderError se) throws SQLException{
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(insert_splider_error);
            int index = 1;
            ps.setString(index++,se.getDatasource());
            ps.setString(index++,se.getTopic());
            ps.setString(index++,se.getDetail());
            ps.setString(index++,se.getUrl());
            ps.setString(index++,se.getErrorresion());
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
            String date = df.format(new Date());
            ps.setString(index++, date);
            ps.setString(index++,se.getTablename());
            ps.execute();
        } catch (SQLException e) {
            logger.error("保存插入splider_error的异常信息失败！\n" + e);
            throw e;
        } finally {
            DBManager.close(null, ps, null);
        }
    }

    public String queryTableName(String tablename,String datasource,Connection conn) throws SQLException{
        PreparedStatement ps = null;
        ResultSet rs = null;
        String counts="0";
        try {
            ps = conn.prepareStatement(query_Table_Name);
            ps.setString(1,tablename);
            ps.setString(2,datasource);
            rs = ps.executeQuery();
            while(rs.next()){
                counts=rs.getString("counts");
            }
        }catch (SQLException e) {
            logger.error("查询splider_tablename数据异常！\n"+e);
            throw e;
        }finally{
            DBManager.close(rs,ps,null);
        }
        return counts;
    }

    public void insertTableName(Connection conn,String tablename,String datasource) throws SQLException{
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(insert_Table_Name);
            int index = 1;
            ps.setString(index++,tablename);
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
            String date = df.format(new Date());
            ps.setString(index++, date);
            ps.setString(index++,datasource);
            ps.execute();
        } catch (SQLException e) {
            logger.error("保存splider_tablename数据的异常信息失败！\n" + e);
            throw e;
        } finally {
            DBManager.close(null, ps, null);
        }
    }
}