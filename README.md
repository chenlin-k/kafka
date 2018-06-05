# kafka
kafka
概述：通过爬虫程序，爬取到百度失信人员的信息进行生产，之后再用消费者将信息取得，再把取得的信息存储到solr、hive当中，并对solr中的数据提供查询
 
HdfsUtils中简单的配置了zookeeper的地址，后期有空再进行修改。

application.properties当中配置了solr的url、core，core是需要进入solr界面进行添加设置，数据库采用的是mysql数据库。
