package com.example.kafka.utils;

/*
数据库连接池管理类，包括初始化，获取连接，关闭连接，该类采用单模式，唯一实例
*/

import org.apache.log4j.Logger;
import org.logicalcobwebs.proxool.ProxoolException;
import org.logicalcobwebs.proxool.configuration.JAXPConfigurator;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;

/**
* 数据库连接池管理类，包括初始化，获取连接，关闭连接，该类采用单模式，唯一实例
* @author loly
*
*/
public class DBManager {
	private static DBManager dbInstanse=null;
	private static Logger logger = Logger.getLogger(DBManager.class);
	/* 数据库别名 */
	public final static String DB_ALIAS_DEFAULT = "approval";
	public final static String DB_ALIAS_SMS = "sms";
	/**
	 * 
	 *
	 */
	private DBManager(){
		try{
			InputStream in = DBManager.class.getResourceAsStream("/dbconfig.xml");
			//InputStream in = DBManager.class.getResourceAsStream("../../../dbconfig.xml");
			JAXPConfigurator.configure(new InputStreamReader(in),false);
//			JAXPConfigurator.configure("config/dbconfig.xml",false);
		}catch(ProxoolException e){
			logger.error("连接池配置失败,"+e.getMessage(),e);
		}
	}
	/**
	 * 获取该类实例
	 * @return
	 */
	public synchronized static DBManager getInstanse(){
		if (dbInstanse == null){
			dbInstanse = new DBManager();
		}
		return dbInstanse;
	}
	/**
	 * 取得数据库连接
	 * @param url
	 * @return con
	 * @throws SQLException
	 */
	public Connection getConnection(String url) throws SQLException{
		if (url == null || "".equals(url)){
			throw new SQLException("url is error!");
		}
		Connection con = DriverManager.getConnection("proxool."+url);
		return con;
	}
	/**
	 * 关闭连接
	 * @param con
	 */
	public static void close(ResultSet set, Statement st, Connection con){
		try{
			if (set != null){
				set.close();
			}
		}catch(SQLException e){
			logger.error("关闭ResultSet异常,"+e.getMessage());
		}
		try{
			if (st != null){
				st.close();
			}
		}catch(SQLException e){
			logger.error("关闭Statement异常,"+e.getMessage());
		}
		try{
			if (con != null && !con.isClosed()){
				con.close();
			}
		}catch(SQLException e){
			logger.error("关闭Connection异常,"+e.getMessage());
		}
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(getInstanse().getConnection(DB_ALIAS_DEFAULT));
	}
}
