package com.example.kafka.hdfs;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.kafka.utils.HiveJdbcCli;
import com.example.kafka.utils.MysqlJT;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HdfsUtils {

    private static final Logger log = Logger.getLogger(HdfsUtils.class);

    /**
     * @author chenlink
     * 创建文件夹,如果不存在
     */
    public void CreateFolder() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.88.127:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/output");

        if (!fs.exists(path)) {
            fs.mkdirs(path);
        }
    }


    /**
     * 功能:将 hdfs://ssmaster:9000/data/paper.txt下载到Windows下c:\paper.txt
     */
    public void Down_Load() {


        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.88.127:8020/");

        FileSystem fs = null;
        Path src = null;
        FSDataInputStream in = null;
        FileOutputStream out = null;

        src = new Path("hdfs://192.168.88.127:8020/user/hive/warehouse/test.txt");

        try {

            fs = FileSystem.get(conf);
            in = fs.open(src);

        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            out = new FileOutputStream("e:\\paper.txt"); //等效  e:/paper.txt
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            IOUtils.copy(in, out);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * @author chenlink
     * 上传本地文件
     */

    public void UploadFile(String filepath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.88.127:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/hive/warehouse/");
        Path src = new Path(filepath);

        fs.copyFromLocalFile(false, true, src, path);
    }


    /**
     * @author chenlink
     * 下载到本地文件
     */
    public void DownFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.88.127:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path hdfs = new Path("/user/hive/warehouse/test.txt");
        Path win10 = new Path("e:/paper_download.txt");

        fs.copyToLocalFile(hdfs, win10);
    }


    /**
     * @author chenlink
     * 删除hdfs文件,如何文件不存在，也运行正常
     */
    public void DeleteFile() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.88.127:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path hdfs = new Path("/user/hive/warehouse/paper.txt");
        fs.delete(hdfs, true);

    }

    /**
     * @author chenlink
     * 显示某个目录下的文件
     */
    public void ListFiles() throws IOException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.88.127:8020/");
        FileSystem fs = FileSystem.get(conf);
        Path hdfs = new Path("/user/hive/warehouse/");


        FileStatus[] files = fs.listStatus(hdfs);
        for (FileStatus file : files) {

            System.out.print(file.getPath().getName());
            System.out.print("\t" + file.isDirectory());
            System.out.print("\t" + file.getOwner());
            System.out.print("\n");
        }

    }

    //"D:\\22.txt"
//    将json写入到txt中
    public void WriteTxt(JSONObject jsonObject, String tabName) {
        int count = 0;
        Statement stmt = null;
        Connection conn = null;
        String filepath = "d:/" + tabName + ".txt";
        File f = new File(filepath);

        try {
            if (f.exists()) {
                System.out.print("文件存在");
            } else {
                System.out.print("文件不存在");

                f.createNewFile();// 不存在则创建
                System.out.print("文件已创建");
            }
            BufferedWriter output = new BufferedWriter(new FileWriter(f, true));//true,则追加写入text文本
            //处理数据
            String keydata = "";
            String loaddata = "";
            Set<String> keys = jsonObject.keySet();
            Iterator iterator1 = keys.iterator();
            Collection<Object> co = jsonObject.values();
            Iterator iterator = co.iterator();
            while (iterator1.hasNext()) {
                String key = (String) iterator1.next();
                if (key.startsWith("_")) {
                    key = key.substring(1, key.length());
                }
                keydata += key + " STRING,";
            }
            keydata = keydata.substring(0, keydata.length() - 1);

            HiveJdbcCli hj = new HiveJdbcCli();
            conn = hj.getConn();
            stmt = conn.createStatement();
            MysqlJT mj = new MysqlJT();
            count = hj.showTables(stmt, tabName);
            if (count == 0 && mj.check(tabName, keydata)) {
                hj.createTable(stmt, tabName, keydata);
                mj.insertSpliderTableName(tabName, "hive");
            }
            while (iterator.hasNext()) {
                String value = iterator.next().toString();
                loaddata += value + ",";
            }
            loaddata = loaddata.substring(0, loaddata.length() - 1);
            output.write(loaddata);
            output.write("\r\n");//换行
            output.flush();
            output.close();

            //上传文件
            HdfsUtils hd = new HdfsUtils();
            hd.UploadFile(filepath);
            System.out.println("上传到hdfs成功！");
            hj.loadData(stmt, tabName);
            System.out.println("加载到hive成功！");
            //把Windows下生成的txt文件删除
            f.delete();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(" hdfs、hive加载数据异常!"+e.getMessage());
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

    //读取txt文件中的内容
    public void ReadTxt(String filePath) {
        try {
            String encoding = "GBK";
            File file = new File(filePath);
            if (file.isFile() && file.exists()) { //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    System.out.println(lineTxt);
                }
                read.close();
            } else {
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        String datasource = "hive";
        String model = "{\"StdStg\":6899, \"StdStl\":8, \"_update_time\":\"1519394115\", \"cambrian_appid\":\"0\", \"changefreq\":\"always\", \"age\":\"37\"}";
        JSONObject jsonObject = new JSONObject();
        HdfsUtils hu = new HdfsUtils();
        String tabName = "test";
        for (int i = 0; i < 4; i++) {
            if (i == 0) {
                datasource = "hive";
                model = "{\"StdStg\":6809, \"StdStl\":7, \"_update_time\":\"1519394115\", \"cambrian_appid\":\"0\", \"changefreq\":\"always\", \"age\":\"38\"}";
                jsonObject = JSON.parseObject(model);
            } else if (i == 1) {
                model = "{\"StdStg\":6810, \"StdStl\":8, \"_update_time\":\"1519394115\", \"cambrian_appid\":\"1\", \"changefreq\":\"always\", \"age\":\"39\"}";
                jsonObject = JSON.parseObject(model);
                datasource = "solr";
            } else if (i == 2) {
                datasource = "hive";
                model = "{\"StdStg\":6811, \"StdStl\":9, \"_update_time\":\"1519394115\", \"cambrian_appid\":\"2\", \"changefreq\":\"always\", \"age\":\"40\"}";
                jsonObject = JSON.parseObject(model);
            } else {
                datasource = "hive";
                model = "{\"StdStg\":6812, \"StdStl\":10, \"_update_time\":\"1519394115\", \"cambrian_appid\":\"3\", \"changefreq\":\"always\", \"age\":\"41\"}";
                jsonObject = JSON.parseObject(model);
            }
            if (datasource != null && !"".equals(datasource)) {
                if (datasource.equals("hive")) {
                    String filepath = "d:/" + tabName + ".txt";
                    HdfsUtils hd = new HdfsUtils();
                    hd.WriteTxt(jsonObject, tabName);
                    System.out.println("hive操作成功！");
                } else if (datasource.equals("solr")) {
                    System.out.println("solr操作成功！");
                }
            }
        }

    }
}
