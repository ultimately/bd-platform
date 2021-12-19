# bd-platform

#### Windows系统安装hadoop环境

1. 安装JDK

2. hadoop解压安装

   ```
   ##hadoop下载地址
   https://hadoop.apache.org/releases.html
   ##下载winutils.exe文件
   https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.2/bin
   ##winutils拷贝到：D:\bigData\hadoop-3.2.2\bin
   ##hadoop.dll拷贝到：C:\Windows\System32
   ```

3. 设置环境变量

   ```
   ##设置系统变量
   HADOOP_HOME
   D:\bigData\hadoop-3.2.2
   ##增加path参数
   %HADOOP_HOME%\bin加入到path里面
   
   ##增加HADOOP_BIN_PATH系统变量，解决hadoop启动yarn报错问题（error Couldn't find a package.json file问题）
   ##没有安装node（yarn工具可忽略，不会影响）
   HADOOP_BIN_PATH
   D:\bigData\hadoop-3.2.2\bin
   
   ```



#### 设置hadoop参数

​	在hadoop目录下新增data/datanode，data/namenode文件夹

![](C:\Users\UI\Desktop\bd-platform\image\1.png)

配置参数

1. D:\bigData\hadoop-3.2.2\etc\hadoop\hadoop-env.cmd

   ```
   ##设置java环境变量
   set JAVA_HOME=C:\PROGRA~1\Java\jdk1.8.0_201
   ```

   

2. D:\bigData\hadoop-3.2.2\etc\hadoop\core-site.xml

   ```
   <configuration>
   	<property>
   		<name>fs.defaultFS</name>
   		<value>hdfs://localhost:9000</value>
   	</property>
   </configuration>
   ```

   

3. D:\bigData\hadoop-3.2.2\etc\hadoop\hdfs-site.xml

   ```
   <configuration>
   
   	<property>
   		<name>dfs.replication</name>
   		<value>1</value>
   	</property>
   	<!-- 去掉HDFS的权限验证. username:hadoop-->
   	<property>
   		<name>dfs.permissions</name>
   		<value>false</value>
   	</property>
   	<property>
   		<name>dfs.http.address</name>
   		<value>localhost:50070</value>
   	</property>
   	<property>
   		<name>dfs.namenode.name.dir</name>
   		<value>/D:/bigData/hadoop-3.2.2/data/namenode</value>
   	</property>
   	<property>
   		<name>dfs.datanode.data.dir</name>
   		<value>/D:/bigData/hadoop-3.2.2/data/datanode</value>
   	</property>
   	
   </configuration>
   ```

   

4. D:\bigData\hadoop-3.2.2\etc\hadoop\mapred-site.xml

   ```
   <configuration>
   
   	<property>
   		<name>mapreduce.framework.name</name>
   		<value>yarn</value>
   	</property>
   
   </configuration>
   ```

   

5. D:\bigData\hadoop-3.2.2\etc\hadoop\yarn-site.xml

   ```
   <configuration>
   
   <!-- Site specific YARN configuration properties -->
   
   	<property>
   		<name>yarn.nodemanager.aux-services</name>
   		<value>mapreduce_shuffle</value>
   	</property>
   	
   	<property>
   		<name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name>
   		<value>org.apache.hadoop.mapred.ShuffleHandler</value>
   	</property>
   
   </configuration>
   ```



#### hadoop启动

1. 查看是否可以正常运行

   ```
   hadoop version
   ```

2. 第一次启动格式化

   ```
   hdfs namenode -format
   
   ##执行成功会在data/namenode文件里会生成一个current文件，则格式化成功。
   ```

3. 启动hadoop

   ```
   D:\bigData\hadoop-3.2.2\sbin\start-all.cmd
   
   ##运行成功后，会出现四个窗口，分别是：yarn-resourcemanager、yarn-nodemanager、hadoop-namenode、hadoop-datanode。
   ```

4. hadoop相关命令

   ```
   ##创建文件夹命令
   hadoop fs -mkdir hdfs://localhost:9000/test/
   
   hadoop fs -mkdir hdfs://localhost:9000/test/testinput
   
   ##上传文件
   hadoop fs -put E:\test01.txt hdfs://localhost:9000/test/testinput
   
   hadoop fs -put E:\test02.txt hdfs://localhost:9000/test/testinput
   
   ##查询命令
   hadoop fs -ls hdfs://localhost:9000/test/testinput
   
   ```

5. hadoop相关端口

   

   ## HDFS端口

   | 参数                      | 描述                          | 默认  | 配置文件       |       例子值        |
   | ------------------------- | ----------------------------- | ----- | -------------- | :-----------------: |
   | fs.default.name namenode  | namenode RPC交互端口          | 9000  | core-site.xml  | hdfs://master:9000/ |
   | dfs.http.address          | NameNode web管理端口          | 50070 | hdfs- site.xml |    0.0.0.0:50070    |
   | dfs.datanode.address      | datanode　控制端口            | 50010 | hdfs -site.xml |    0.0.0.0:50010    |
   | dfs.datanode.ipc.address  | datanode的RPC服务器地址和端口 | 50020 | hdfs-site.xml  |    0.0.0.0:50020    |
   | dfs.datanode.http.address | datanode的HTTP服务器和端口    | 50075 | hdfs-site.xml  |    0.0.0.0:50075    |
   |                           |                               |       |                |                     |

    

   ## MR端口

   |               参数               | 描述                   | 默认  | 配置文件        | 例子值              |
   | :------------------------------: | ---------------------- | ----- | --------------- | ------------------- |
   |        mapred.job.tracker        | job-tracker交互端口    | 8021  | mapred-site.xml | hdfs://master:8021/ |
   |               job                | tracker的web管理端口   | 50030 | mapred-site.xml | 0.0.0.0:50030       |
   | mapred.task.tracker.http.address | task-tracker的HTTP端口 | 50060 | mapred-site.xml | 0.0.0.0:50060       |

    

   ## 其它端口

   |            参数            | 描述                           | 默认  | 配置文件      | 例子值        |
   | :------------------------: | ------------------------------ | ----- | ------------- | ------------- |
   | dfs.secondary.http.address | secondary NameNode web管理端口 | 50090 | hdfs-site.xml | 0.0.0.0:50090 |
   |                            |                                |       |               |               |
   |                            |                                |       |               |               |
