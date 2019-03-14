# 数据集

统一生成的数据集为 Record 类型，有三个字段：

```
String tag
long timestamp
List<Object> fields
```

不同数据集的 tag 和 fields 的含义不同：

* NOAA 
	* tag：STN_WBAN，如：010230_99999
	* fields：13个
* GeoLife
	* tag：文件名中的人员编号，即：0~179
	* fields：4 个：经、纬、0、海拔
* mlab
	* tag：json 中 metric、hostname、experiment 用 . 拼接
	* fields：只有一个，就叫 value

## 数据库 

* SummaryStore 和 WaterWheel 只能用 NOAA 和 GeoLife 数据集，用 long 表示时间序列 id


## Waterwheel

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

cn.edu.thu.database.waterwheel.WaterWheelM 类为拓扑类

* local 模式
	* 将 pom 中的 storm 的 scope 注释掉
	* 运行 cn.edu.thu.database.waterwheel.WaterWheelM，提交拓扑到本地
	* 运行 MainLoad
	* 运行 MainQuery

* 集群模式
	* 将 pom 中的 storm 的 scope 打开
	* 启动 HDFS，Zookeeper，Storm
	* 给 cn.edu.thu.manager.WaterWheel 打包
	* ./bin/storm jar topology.jar mainclass 将拓扑提价到storm上
	* 运行 MainLoad 导入数据（数据在 hdfs://waterwheel下，一定大小才会刷磁盘）
