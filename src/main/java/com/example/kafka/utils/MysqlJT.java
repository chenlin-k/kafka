package com.example.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.kafka.hdfs.HdfsUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * @author chenpeng
 * @description 采集数据插入到mysql，采集数据时，封装的json中tablename的value设计时需要查询splider_tablename表中，保证每个数据源设计的表名都唯一
 * @Date: Create in 13:39 2018/2/28
 */
public class MysqlJT {

    private static Logger logger = Logger.getLogger(MysqlJT.class);
    private Connection con1;
    private Connection con2;

    private PreparedStatement ps1 ;
    private PreparedStatement ps2 ;

    public void createTable(String tabName,String datasource, JSONObject jsonObject ) {
        try {
            Set<String> keys = jsonObject.keySet();
            Iterator iterator1 = keys.iterator();
            Collection<Object> co=jsonObject.values();
            Iterator iterator2 =co.iterator();
            // 首先要获取连接，即连接到数据库
            con1 = DBManager.getInstanse().getConnection(DBManager.DB_ALIAS_DEFAULT);
            con2 = DBManager.getInstanse().getConnection(DBManager.DB_ALIAS_DEFAULT);
            String create_table = "create table "+tabName+"(id bigint auto_increment primary key not null";
            String insert_table = "insert into "+tabName+"(";
            while(iterator1.hasNext()){
                String key = (String) iterator1.next();
                create_table+=","+key+" varchar(255)";
                insert_table+=key+",";
            }
            create_table+=")DEFAULT CHARSET=utf8";
            insert_table=insert_table.substring(0,insert_table.length()-1);
            insert_table+=") values( ";
            while(iterator2.hasNext()){
                String value=iterator2.next().toString();
                insert_table+="'"+value+"',";
            }
            insert_table=insert_table.substring(0,insert_table.length()-1);
            insert_table+=")";
            System.out.println("-------------------------------------------------------");
            System.out.println("createt_table_sql:"+create_table);
            System.out.println("insert_table_sql:"+insert_table);

            ps1 = con1.prepareStatement(create_table);
            ps2 = con2.prepareStatement(insert_table);
            if(check(tabName,datasource)){
                //创建表
                ps1.execute();
                //插入数据到splider_tablename
                insertSpliderTableName(tabName,datasource);
            }
            //插入数据到table中
            ps2.execute();
            DBManager.close(null, ps2, con2);
            DBManager.close(null, ps1, con1);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("Mysql数据采集保存出现异常："+e.getMessage());
        }
    }

    /*
    是否需要创建表
     */
    public boolean check(String tablename,String datasource)  {
        Connection con = null;
        String counts="0";
        try {
            con = DBManager.getInstanse().getConnection(DBManager.DB_ALIAS_DEFAULT);
        JDBCConnetct jd=new JDBCConnetct();
        counts=jd.queryTableName(tablename,datasource,con);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DBManager.close(null, null, con);
        }
        if(counts.equals("0")){
            return true;
        }
        return false;
    }
    /*
    记录mysql创建的采集数据源表名
     */
    public void insertSpliderTableName(String tablename,String datasource) {
        Connection con = null;
        try {
            con = DBManager.getInstanse().getConnection(DBManager.DB_ALIAS_DEFAULT);
            JDBCConnetct jd = new JDBCConnetct();
            jd.insertTableName(con, tablename, datasource);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DBManager.close(null, null, con);
        }
    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        HiveJdbcCli hj=new HiveJdbcCli();
//        hj.createTable("test",jsonObject);
//        MysqlJT mj=new MysqlJT();
//        mj.createTable("test","mysql",jsonObject);
    }
}
