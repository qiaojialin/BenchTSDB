# Waterwheel

## 示例程序

示例代码在 cn.edu.thu.manager.waterwheelsample 包下

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

	
## 正式测试

cn.edu.thu.manager.WaterWheel 类为拓扑类

* local 模式
	* 将 pom 中的 storm 的 scope 注释掉
	* 运行 cn.edu.thu.manager.WaterWheel，提交拓扑到本地
	* 运行 MainLoad
	* 运行 MainQuery

* 集群模式
	* 将 pom 中的 storm 的 scope 打开
	* 启动 HDFS，Zookeeper，Storm
	* 给 cn.edu.thu.manager.WaterWheel 打包
	* ./bin/storm jar topology.jar mainclass 将拓扑提价到storm上
	* 运行 MainLoad 导入数据（数据在 hdfs://waterwheel下，一定大小才会刷磁盘）
	* 运行 MainQuery 查询数据