# 写入测试

pom 中指定主类 cn.edu.thu.MainLoad

打包 mvn clean package

配置文件为 conf/config.properties

```$xslt
DATA_SET 为数据集类型
DATA_DIR 为数据集位置
FILE_PATH 为写出的文件位置
```

启动命令：

```
java -jar BenchTSDB-1.0-jar-with-dependencies.jar conf/config.properties
```

# 查询测试

pom 中指定主类 cn.edu.thu.MainQuery

打包 mvn clean package

配置文件为 conf/config.properties

```$xslt
DATA_SET 为数据集类型
FILE_PATH 查询的文件位置
QUERY_TAG 查询的设备ID
FIELD 查询的测点名字
START_TIME 查询的起始时间：long 或 min
END_TIME 查询的终止时间：long 或 max
```

# 数据集

统一生成的数据集为 Record 类型，有三个字段：

```
String tag
long timestamp
List<Object> fields
```

不同数据集的 tag 和 fields 的含义不同：

* NOAA 
	* tag: STN_WBAN，如：010230_99999
	* fields: 13个
* GeoLife
	* tag: 文件名中的人员编号，即：0~179
	* fields: Latitude, Longitude, Zero, Altitude
* TDrive
  * tag：文件名去掉 .txt
  * fields: longitude, latitude
* Redd
  * tag: house_1_channel_1
  * value: value

## 数据库 

* SummaryStore 和 WaterWheel 只能用 NOAA 和 GeoLife 数据集，用 long 表示时间序列 id


## Waterwheel

### 集群部署

../storm-1.2.2/bin/storm jar waterwheel.jar cn.edu.thu.database.waterwheel.WaterWheelManager config.properties

部署上去之后 server 的 ip 是 supervisor 节点中任意一个。

### 示例程序

示例代码在 cn.edu.thu.database.waterwheel 包下

* local 模式
	* 将 pom 中的 storm 的 scope 注释掉
	* 将 GeoSubmit 中的 local 设为 true
	* 运行 GeoSubmit，提交作业到 storm local模式
	* 运行 GeoLoad，导入示例数据
	* 运行 GeoQuery，聚合查询
* 集群模式
	* 将 pom 中的 storm 的 scope 打开
	* 启动 HDFS，Zookeeper，Storm
	* mvn clean package
	* ./bin/storm jar topology.jar mainclass 将拓扑提价到storm上
	* 运行 GeoLoad，导入示例数据（数据在 hdfs://waterwheel下，一定大小才会刷磁盘）
	* 运行 GeoQuery，聚合查询

	
### 正式测试

cn.edu.thu.database.waterwheel.WaterWheelManager 类为拓扑类

* local 模式
	* 将 pom 中的 storm 的 scope 注释掉
	* 运行 cn.edu.thu.database.waterwheel.WaterWheelManager，提交拓扑到本地
	* 运行 MainLoad
	* 运行 MainQuery

* 集群模式
	* 将 pom 中的 storm 的 scope 打开
	* 启动 HDFS，Zookeeper，Storm
	* 给 cn.edu.thu.manager.WaterWheel 打包
	* ./bin/storm jar topology.jar mainclass 将拓扑提价到storm上
	* 运行 MainLoad 导入数据（数据在 hdfs://waterwheel下，一定大小才会刷磁盘）
