package com.example.kafka.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;  
import java.sql.ResultSet;  
import java.sql.SQLException;  
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

/** 
 * Hive的JavaApi 
 *  
 * 启动hive的远程服务接口命令行执行：hive --service hiveserver >/dev/null 2>/dev/null & 
 *  
 * @author chenlink
 *  
 */  
public class HiveJdbcCli {  
  
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://192.168.88.127:10000/default";
    private static String user = "hive";  
    private static String password = "admin";
    private static String sql = "";  
    private static ResultSet res;  
    private static final Logger log = Logger.getLogger(HiveJdbcCli.class);  

        public static void main(String[] args) throws SQLException, IOException {
            Connection conn = null;
            Statement stmt = null;
            int cont=0;
            HiveJdbcCli hj = new HiveJdbcCli();
            try {
                conn = hj.getConn();
                stmt = conn.createStatement();
                String tabName="baidushixin";
//                String keys="";
//                // 第一步:查看需要创建的表是否存在
                cont=hj.showTables(stmt, tabName);
//                if(cont!=0){
//                    // 第二步:不存在就创建
//                    hj.createTable(stmt, tabName, keys);
//                }
                // 执行load data into table操作,加载数据进数据库
//                hj.loadData(stmt, tabName,filepath);
                //删除表
//                String tableName = hj.dropTable(stmt,"test");
//                 执行describe table操作，查看表结构
//                hj.describeTables(stmt, "test");
//                 执行 select * query 操作
                hj.selectData(stmt, "baidushixin");
//                 执行 regular hive query 统计操作
//                hj.countData(stmt, "test");

            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                log.error(driverName + " not found!", e);
                System.exit(1);
            } catch (SQLException e) {
                e.printStackTrace();
                log.error("Connection error!", e);
                System.exit(1);
            } finally {
                try {
                    if (stmt != null) {
                        stmt.close();
                        stmt = null;
                    }
                    if (conn != null) {
                        conn.close();
                        conn = null;
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    private void countData(Statement stmt, String tableName)
            throws SQLException {  
        sql = "select count(1) from " + tableName;  
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);  
        System.out.println("执行“regular hive query”运行结果:");  
        while (res.next()) {  
            System.out.println("count ------>" + res.getString(1));  
        }  
    }

    private void selectData(Statement stmt, String tableName)
            throws SQLException {  
        sql = "select * from " + tableName;  
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);  
        System.out.println("执行 select * query 运行结果:");  
        while (res.next()) {  
            System.out.println(res.getString(1) + "," + res.getString(2)+","+res.getString(3)+","+res.getString(4)+","+res.getString(5)+","+res.getString(6));
        }  
    }

    public void loadData(Statement stmt, String tableName)
            throws SQLException {  
        String filepath = "/user/hive/warehouse/"+tableName+".txt";
        sql = "load data inpath '" + filepath + "' into table  "
                + tableName+" partition (name='"+tableName+"')";
        System.out.println("Running:" + sql);  
        stmt.execute(sql);
    }

    private void describeTables(Statement stmt, String tableName)
            throws SQLException {  
        sql = "describe " + tableName;  
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);  
        System.out.println("执行 describe table 运行结果:");
        int i=1;
        while (res.next()) {
            System.out.println(res.getString(i) );
            System.out.println("-----------------------");
        }  
    }

    public int showTables(Statement stmt, String tableName)
            throws SQLException {  
        sql = "show tables '" + tableName + "'";
        int row=0;
        System.out.println("Running:" + sql);  
        res = stmt.executeQuery(sql);
        System.out.println("执行 show tables 运行结果:");
        if (res.next()) {
            System.out.println(res.getString(1));
            row++;
        }
        System.out.println("---------------"+row);
        return res.getRow();
    }

    public void createTable(Statement stmt,String tableName,String keys )
            throws SQLException {
        sql="create table "+tableName+" ("+keys+") partitioned by(name STRING) row format delimited fields terminated by ',' LINES TERMINATED BY '\n'stored as TEXTFILE";
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        System.out.println(sql);
        stmt.execute(sql);
    }

    private String dropTable(Statement stmt,String tableName ) throws SQLException {
        // 创建的表名  
        sql = "drop table " + tableName;
        System.out.println("执行 drop table ...");
        stmt.execute(sql);
        return tableName;  
    }

    public Connection getConn() throws ClassNotFoundException,
            SQLException {  
        Class.forName(driverName);  
        Connection conn = DriverManager.getConnection(url, user, password);  
        return conn;  
    }  
  
} 