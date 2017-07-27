import org.apache.spark.sql.hive.HiveContext  //声明调用hivecontext
val hiveContext = new HiveContext(sc)  //新建hiveContext() 函数
val iris = hiveContext.sql("select * from iris limit 10")  //调用函数，使用sql语句
hiveContext.sql("drop table if exists wjh.flights")
iris.show() //查询结果
hiveContext.sql("select * from iris limit 10").show()    //以上2条等同于这个
val joindata = data.join(airlinesdf,data("uniquecarrier") === airlinesdf("Code"),"left_outer")    //join表联合查询（左联合）
val addgaindata = joindata.withColumn("gain",joindata("depdelay")-joindata("arrdelay"))				//withColumn函数新增列
addgaindata.registerTempTable("addgaindata")							//生成一个临时表（存储于内存中）
val model_data = hiveContext.sql("select year, month, arrdelay, depdelay, distance, uniquecarrier, description, gain from addgaindata")



var xx = sc.textFile("/user/impala/test/xx.txt")  //xx.txt已经是上传至hdfs的文件
xx.first()                                        //显示xx文件的第一行(RDD文件)
val count = xx.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_+_)    //将xx文本根据空格 分割为一个单词
xx.filter(line => line.contains("good")).count()   /// 有多少行含有good
xx.count()											// RDD中有多少行  
count.collect()    //显示结果










