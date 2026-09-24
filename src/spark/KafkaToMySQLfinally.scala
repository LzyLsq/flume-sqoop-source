package orderProcessing  // 定义包名，用于组织和分类代码

import org.apache.kafka.common.serialization.StringDeserializer  // 引入 Kafka 的字符串反序列化器
import org.apache.log4j.{Level, Logger}  // 引入日志级别和日志记录器，用于控制日志输出
import org.apache.spark.SparkConf  // 引入 Spark 配置类，用于设置 Spark 应用程序的配置参数
import org.apache.spark.sql.expressions.UserDefinedFunction  // 引入用户自定义函数接口，用于定义自定义的 Spark SQL 函数
import org.apache.spark.sql.functions._  // 引入 Spark SQL 函数库，提供各种数据处理函数
import org.apache.spark.sql.types._  // 引入 Spark 数据类型库，用于定义数据框的结构和数据类型
import org.apache.spark.sql.{DataFrame, SparkSession}  // 引入数据框和 Spark 会话，用于数据处理和 Spark 应用程序的入口
import org.apache.spark.streaming._  // 引入 Spark Streaming 相关类，用于处理流数据
import org.apache.spark.streaming.kafka010._  // 引入 Kafka 相关的 Spark Streaming 类，用于从 Kafka 消费数据

import java.sql.{Connection, DriverManager, PreparedStatement}  // 引入 Java SQL 相关类，用于与 MySQL 数据库进行交互

object KafkaToMySQL {  // 定义主对象，作为程序的入口
  val logger = Logger.getLogger(getClass.getName)  // 获取日志记录器实例，用于记录日志信息

  // 敏感配置一律从环境变量读取；缺失时直接失败，避免把账号口令硬编码进仓库
  private def requireEnv(name: String): String =  // 定义一个私有方法，用于从环境变量中读取必需的配置项
    Option(System.getenv(name)).getOrElse(  // 尝试获取环境变量，如果不存在则抛出异常
      throw new IllegalStateException(  // 抛出非法状态异常，提示缺少环境变量
        "缺少必需的环境变量 " + name + "，请参考 README「配置项（环境变量）」一节"  // 异常信息，提示用户查看文档
      )
    )

  // MySQL 配置（JDBC URL / 账号 / 密码均来自环境变量）
  val dbConfig = Map(  // 定义 MySQL 数据库的配置信息
    "url" -> requireEnv("MYSQL_JDBC_URL"),  // 从环境变量读取 JDBC 连接 URL
    "user" -> requireEnv("DB_USER"),  // 从环境变量读取数据库用户名
    "password" -> requireEnv("DB_PASSWORD"),  // 从环境变量读取数据库密码
    "driver" -> "com.mysql.cj.jdbc.Driver"  // 数据库的 JDBC 驱动类
  )

  def main(args: Array[String]): Unit = {  // 定义主函数，程序的入口点
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)  // 设置 Spark 的日志级别为警告，减少不必要的日志输出
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)  // 设置 Kafka 的日志级别为警告

    val conf = new SparkConf()  // 创建 Spark 配置对象
      .setAppName("KafkaToMySQL")  // 设置 Spark 应用程序的名称
      .setMaster("local[*]")  // 设置 Spark 应用程序的执行模式为本地模式，使用所有可用的核心
      .set("spark.sql.shuffle.partitions", "2")  // 设置 Spark SQL 的 shuffle 分区数为 2
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // 设置 Spark 的序列化器为 Kryo 序列化器，提高序列化效率

    val ssc = new StreamingContext(conf, Seconds(10))  // 创建 Spark Streaming 上下文，设置批次间隔为 10 秒
    val spark = SparkSession.builder  // 创建 Spark 会话构建器
      .config(conf)  // 使用之前定义的 Spark 配置
      .getOrCreate()  // 获取或创建 Spark 会话实例
    import spark.implicits._  // 导入 Spark 会话的隐式转换，方便后续数据处理

    // 注册余弦相似度 UDF
    val cosineSimilarityUDF = udf((vec1: Seq[Double], vec2: Seq[Double]) => {  // 定义一个用户自定义函数，计算两个向量的余弦相似度
      if (vec1.isEmpty || vec2.isEmpty) 0.0  // 如果任意向量为空，返回 0.0
      else {
        val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum  // 计算两个向量的点积
        val normA = math.sqrt(vec1.map(x => x * x).sum)  // 计算第一个向量的模长
        val normB = math.sqrt(vec2.map(x => x * x).sum)  // 计算第二个向量的模长
        dotProduct / (normA * normB)  // 计算并返回余弦相似度
      }
    })

    // Kafka 消费者配置
    val kafkaParams = Map[String, Object](  // 定义 Kafka 消费者的配置参数
      "bootstrap.servers" -> requireEnv("KAFKA_BOOTSTRAP_SERVERS"),  // Kafka 服务器的地址和端口
      "key.deserializer" -> classOf[StringDeserializer],  // 消息键的反序列化器类
      "value.deserializer" -> classOf[StringDeserializer],  // 消息值的反序列化器类
      "group.id" -> "niit",  // 消费者组的 ID
      "auto.offset.reset" -> "latest",  // 自动偏移重置策略，设置为 latest 表示从最新的偏移开始消费
      "enable.auto.commit" -> (false: java.lang.Boolean)  // 是否启用自动提交偏移
    )

    val topics = Array("orders", "users")  // 定义要消费的 Kafka 主题数组

    val stream = KafkaUtils.createDirectStream[String, String](  // 创建直接的 Kafka 数据流
      ssc,  // Spark Streaming 上下文
      LocationStrategies.PreferConsistent,  // 位置策略，优先选择一致的分区分配
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)  // 消费策略，订阅指定的主题并使用之前的消费者配置
    )

    // 定义 数据结构
    val orderSchema = new StructType()  // 定义订单数据的结构
      .add("order_category", StringType)  // 添加订单类别字段，类型为字符串
      .add("order_name", StringType)  // 添加订单名称字段，类型为字符串
      .add("order_quantity", IntegerType)  // 添加订单数量字段，类型为整数
      .add("order_date", StringType)  // 添加订单日期字段，类型为字符串
      .add("is_valid", StringType)  // 添加是否有效字段，类型为字符串

    val userSchema = new StructType(orderSchema.fields)  // 基于订单数据结构创建用户数据结构
      .add("user_id", IntegerType)  // 添加用户 ID 字段，类型为整数
      .add("rating", IntegerType)  // 添加评分字段，类型为整数

    stream.foreachRDD { rdd =>  // 对每个 RDD（批次数据）进行处理
      try {
        if (!rdd.isEmpty()) {  // 如果 RDD 不为空
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges  // 获取偏移范围信息

          // 处理订单数据
          val df = rdd.map(record => record.value)  // 将 RDD 中的记录映射为消息值
            .toDF("value")  // 转换为数据框，设置列为 "value"
            .withColumn("parsed_data", from_json(col("value"), orderSchema))  // 将 JSON 字符串解析为结构化数据
            .filter(col("parsed_data").isNotNull)  // 过滤掉解析失败的记录
            .select("parsed_data.*")  // 选择解析后的所有字段

          val cleanedDF = cleanData(df.cache())  // 对数据进行清洗，并缓存清洗后的数据框
          cleanedDF.createOrReplaceTempView("order_data")  // 创建临时视图，方便后续查询
          println("\n===== cleanedDF 数据 =====")  // 打印分隔符和标题
          cleanedDF.show(truncate = false)  // 显示清洗后的数据框内容，不截断
          writeDataToMySQL(cleanedDF.select(  // 将清洗后的订单数据写入 MySQL 的 "orders" 表
            col("order_category"),
            col("order_name"),
            col("order_quantity"),
            col("order_date"),
            col("is_valid")
          ), "orders")

          if (!cleanedDF.isEmpty) {  // 如果清洗后的数据框不为空
            cleanedDF.printSchema()  // 打印数据框的结构
            cleanedDF.show(5, truncate = false)  // 显示数据框的前 5 条记录，不截断
          }

          // 处理用户数据
          val userDF = rdd.map(record => record.value)  // 将 RDD 中的记录映射为消息值
            .toDF("value")  // 转换为数据框，设置列为 "value"
            .withColumn("parsed_data", from_json(col("value"), userSchema))  // 将 JSON 字符串解析为结构化数据
            .filter(col("parsed_data").isNotNull)  // 过滤掉解析失败的记录
            .select("parsed_data.*")  // 选择解析后的所有字段

          val userCleanedDF = cleanUserRatingData(userDF.cache())  // 对用户数据进行清洗，并缓存清洗后的数据框
          println("\n===== userCleanedDF 数据 =====")  // 打印分隔符和标题
          userCleanedDF.show(truncate = false)  // 显示清洗后的用户数据框内容，不截断
          writeDataToMySQL(userCleanedDF, "users")  // 将清洗后的用户数据写入 MySQL 的 "users" 表

          // 原有统计功能
          val totalOrdersDF = executeOriginalStatistics(spark, cleanedDF)  // 执行原始的统计功能
          println("\n===== totalOrdersDF 数据 =====")  // 打印分隔符和标题
          totalOrdersDF.show(truncate = false)  // 显示统计结果数据框内容，不截断

          // 推荐功能
          val orderCategoryMatrixDF = executeRecommendations(spark, cleanedDF, userCleanedDF, cosineSimilarityUDF)  // 执行推荐功能
          println("\n===== orderCategoryMatrixDF 数据 =====")  // 打印分隔符和标题
          orderCategoryMatrixDF.show(truncate = false)  // 显示推荐结果数据框内容，不截断

          cleanedDF.unpersist()  // 解除缓存的订单数据框
          userCleanedDF.unpersist()  // 解除缓存的用户数据框
          stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)  // 异步提交偏移范围
        }
      } catch {
        case e: Exception =>  // 捕获异常
          logger.error("Stream processing failed", e)  // 记录错误日志
          ssc.stop(stopSparkContext = true, stopGracefully = true)  // 停止 Spark Streaming 上下文
          spark.stop()  // 停止 Spark 会话
      }
    }

    sys.addShutdownHook {  // 添加关机钩子，用于在程序退出时执行清理操作
      ssc.stop(stopSparkContext = true, stopGracefully = true)  // 停止 Spark Streaming 上下文
      spark.stop()  // 停止 Spark 会话
    }

    ssc.start()  // 启动 Spark Streaming 上下文
    ssc.awaitTerminationOrTimeout(12000000)  // 等待 Spark Streaming 上下文终止或超时（12000000 毫秒）
    ssc.stop(stopSparkContext = true, stopGracefully = true)  // 停止 Spark Streaming 上下文
    spark.stop()  // 停止 Spark 会话
  }

  // 统计 功能封装
  private def executeOriginalStatistics(spark: SparkSession, cleanedDF: DataFrame): DataFrame = {  // 定义执行原始统计功能的方法
    import spark.implicits._  // 导入 Spark 会话的隐式转换

    // 1. 实时统计所有有效和无效订单号的总和
    val totalOrdersDF = cleanedDF.groupBy("is_valid")  // 按是否有效字段分组
      .agg(count("*").alias("total_count"))  // 聚合计算每组的记录数，并重命名为 "total_count"
    println("\n===== total_orders 数据 =====")  // 打印分隔符和标题
    totalOrdersDF.show(truncate = false)  // 显示统计结果数据框内容，不截断
    writeDataToMySQL(totalOrdersDF, "total_orders")  // 将统计结果写入 MySQL 的 "total_orders" 表

    // 2. 实时统计各个订单号各自的有效和无效数量
    val orderStatsDF = cleanedDF.groupBy("order_name", "is_valid")  // 按订单名称和是否有效字段分组
      .agg(
        count("*").alias("count_per_order"),  // 聚合计算每组的记录数，并重命名为 "count_per_order"
        sum("order_quantity").alias("total_quantity")  // 聚合计算每组的订单数量总和，并重命名为 "total_quantity"
      )
    println("\n===== order_stats 数据 =====")  // 打印分隔符和标题
    orderStatsDF.show(truncate = false)  // 显示统计结果数据框内容，不截断
    writeDataToMySQL(orderStatsDF, "order_stats")  // 将统计结果写入 MySQL 的 "order_stats" 表

    // 3. 实时统计所有订单类别的数量
    val categoryStatsDF = cleanedDF.groupBy("order_category")  // 按订单类别字段分组
      .agg(
        count("*").alias("category_count"),  // 聚合计算每组的记录数，并重命名为 "category_count"
        avg("order_quantity").alias("avg_quantity")  // 聚合计算每组的订单数量平均值，并重命名为 "avg_quantity"
      )
    println("\n===== category_stats 数据 =====")  // 打印分隔符和标题
    categoryStatsDF.show(truncate = false)  // 显示统计结果数据框内容，不截断
    writeDataToMySQL(categoryStatsDF, "category_stats")  // 将统计结果写入 MySQL 的 "category_stats" 表

    // 4. 使用 Spark SQL 统计
    val sqlStatsDF = spark.sql(  // 执行 Spark SQL 查询
      """SELECT order_name,
        |SUM(CASE WHEN is_valid = 'Y' THEN 1 ELSE 0 END) AS valid_count,
        |SUM(CASE WHEN is_valid = 'N' THEN 1 ELSE 0 END) AS invalid_count
        |FROM order_data
        |GROUP BY order_name""".stripMargin
    )
    println("\n===== sql_stats 数据 =====")  // 打印分隔符和标题
    sqlStatsDF.show(truncate = false)  // 显示统计结果数据框内容，不截断
    writeDataToMySQL(sqlStatsDF, "sql_stats")  // 将统计结果写入 MySQL 的 "sql_stats" 表

    // 5. 使用 Spark Core/RDD 统计
    val rddStatsDF = cleanedDF.rdd  // 将数据框转换为 RDD
      .map(row => (  // 映射每一行，提取订单名称、订单类别和是否有效字段
        row.getString(1),  // order_name
        row.getString(0),  // order_category
        row.getString(4)  // is_valid
      ))
      .map(t => ((t._1, t._2, t._3), 1))  // 映射为键值对，键为 (order_name, order_category, is_valid)，值为 1
      .reduceByKey(_ + _)  // 按键聚合，累加值
      .map { case ((name, category, valid), count) =>  // 映射回原来的字段结构
        (name, category, valid, count)
      }
      .toDF("order_name", "order_category", "is_valid", "count")  // 转换回数据框，并设置列名
    println("\n===== rdd_stats 数据 =====")  // 打印分隔符和标题
    rddStatsDF.show(truncate = false)  // 显示统计结果数据框内容，不截断
    writeDataToMySQL(rddStatsDF, "rdd_stats")  // 将统计结果写入 MySQL 的 "rdd_stats" 表

    totalOrdersDF  // 返回总订单统计结果数据框
  }

  // 推荐功能封装
  private def executeRecommendations(spark: SparkSession,
                                     cleanedDF: DataFrame,
                                     userCleanedDF: DataFrame,
                                     cosineSimilarityUDF: UserDefinedFunction): DataFrame = {  // 定义执行推荐功能的方法
    import spark.implicits._  // 导入 Spark 会话的隐式转换

    val validDF = cleanedDF.filter(col("is_valid") === "Y")  // 筛选有效的订单数据
    val userValidDF = userCleanedDF.filter(col("is_valid") === "Y")  // 筛选有效的用户数据

    // a. 订单-类别关系矩阵（累加类别在订单中的出现次数）
    val orderCategoryMatrixDF = validDF
      .select("order_name", "order_category")  // 选择订单名称和订单类别字段
      .groupBy("order_name", "order_category")  // 按订单名称和订单类别分组
      .agg(count("*").alias("category_count"))  // 聚合计算每组的记录数，并重命名为 "category_count"
      .groupBy("order_name")  // 按订单名称分组
      .pivot("order_category")  // 将订单类别字段进行透视
      .agg(sum("category_count").alias("total_count"))  // 聚合计算每个订单名称下每个类别的总次数，并重命名为 "total_count"
      .na.fill(0)  // 用 0 填充空值
    println("\n===== order_category_matrix 数据 =====")  // 打印分隔符和标题
    orderCategoryMatrixDF.show(truncate = false)  // 显示订单-类别关系矩阵数据框内容，不截断
    writeDataToMySQL(orderCategoryMatrixDF, "order_category_matrix")  // 将订单-类别关系矩阵写入 MySQL 的 "order_category_matrix" 表

    // b. 类别-类别 关系矩阵（使用余弦相似度）
    val categoryItemMatrix = validDF
      .select("order_name", "order_category")  // 选择订单名称和订单类别字段
      .distinct()  // 去重
      .groupBy("order_category")  // 按订单类别分组
      .pivot("order_name")  // 将订单名称字段进行透视
      .agg(count("*"))  // 聚合计算每个类别下每个订单名称的出现次数
      .na.fill(0)  // 用 0 填充空值

    val categoryVectorsDF = categoryItemMatrix.select(  // 选择类别和对应的特征向量
      col("order_category"),
      array(categoryItemMatrix.columns.filter(_ != "order_category").map(col): _*).cast("array<double>").alias("features")
    )

    val categorySimilarityDF = categoryVectorsDF.alias("cat1")  // 为类别特征数据框设置别名 "cat1"
      .join(categoryVectorsDF.alias("cat2"), col("cat1.order_category") < col("cat2.order_category"))  // 与自身连接，筛选类别对
      .select(
        col("cat1.order_category").alias("category_a"),  // 选择第一个类别的名称，重命名为 "category_a"
        col("cat2.order_category").alias("category_b"),  // 选择第二个类别的名称，重命名为 "category_b"
        cosineSimilarityUDF(col("cat1.features"), col("cat2.features")).alias("cosine_similarity")  // 计算两个类别特征向量的余弦相似度，重命名为 "cosine_similarity"
      )
      .groupBy("category_a", "category_b")  // 按类别对分组
      .agg(avg("cosine_similarity").alias("average_score"))  // 聚合计算每组的平均余弦相似度，重命名为 "average_score"

    // 计算 association_count 和平均 average_score
    val categoryPairsDF = categorySimilarityDF.groupBy("category_a", "category_b")  // 按类别对分组
      .agg(
        count("*").alias("association_count"),  // 聚合计算每组的关联次数，重命名为 "association_count"
        avg("average_score").alias("average_score")  // 聚合计算每组的平均相似度，重命名为 "average_score"
      )

    println("\n===== category_category_matrix 数据 =====")  // 打印分隔符和标题
    categoryPairsDF.show(truncate = false)  // 显示类别-类别关系矩阵数据框内容，不截断
    writeDataToMySQL(categoryPairsDF, "category_category_matrix")  // 将类别-类别关系矩阵写入 MySQL 的 "category_category_matrix" 表

    // c. Item-Based CF（使用余弦相似度）
    val categoryFeaturesDF = validDF.groupBy("order_category")  // 按订单类别分组
      .pivot("order_name")  // 将订单名称字段进行透视
      .agg(avg("order_quantity"))  // 聚合计算每个类别下每个订单名称的平均订单数量
      .na.fill(0)  // 用 0 填充空值

    val categoryVectorsDFItem = categoryFeaturesDF.select(  // 选择类别和对应的特征向量
      col("order_category"),
      array(categoryFeaturesDF.columns.filter(_ != "order_category").map(col): _*).cast("array<double>").alias("features")
    )

    val categorySimilarityDFItem = categoryVectorsDFItem.alias("cat1")  // 为类别特征数据框设置别名 "cat1"
      .join(categoryVectorsDFItem.alias("cat2"), col("cat1.order_category") < col("cat2.order_category"))  // 与自身连接，筛选类别对
      .select(
        col("cat1.order_category").alias("category_a"),  // 选择第一个类别的名称，重命名为 "category_a"
        col("cat2.order_category").alias("category_b"),  // 选择第二个类别的名称，重命名为 "category_b"
        cosineSimilarityUDF(col("cat1.features"), col("cat2.features")).alias("cosine_similarity")  // 计算两个类别特征向量的余弦相似度，重命名为 "cosine_similarity"
      )

    val itemBasedCFRatingDF = categorySimilarityDFItem
      .join(  // 与用户评分数据连接
        userValidDF.select(
          col("order_category").alias("category"),  // 选择订单类别字段，重命名为 "category"
          col("user_id"),  // 选择用户 ID 字段
          col("rating")  // 选择评分字段
        ),
        expr("category_a = category"),  // 连接条件：类别 A 等于订单类别
        "left_outer"  // 左外连接
      )
      .groupBy("category_a")  // 按类别 A 分组
      .agg(
        concat_ws(",", collect_set("category_b")).alias("recommended_categories"),  // 聚合收集类别 B 并用逗号连接，重命名为 "recommended_categories"
        concat_ws(",", collect_list(col("cosine_similarity").cast("string"))).alias("similarity_scores"),  // 聚合收集余弦相似度并用逗号连接，重命名为 "similarity_scores"
        avg("rating").cast("int").alias("rating_score")  // 聚合计算平均评分并转换为整数，重命名为 "rating_score"
      )
      .withColumnRenamed("category_a", "main_category")  // 将类别 A 列重命名为 "main_category"
    println("\n===== item_based_cf 数据 =====")  // 打印分隔符和标题
    itemBasedCFRatingDF.show(truncate = false)  // 显示基于物品的协同过滤评分数据框内容，不截断
    writeDataToMySQL(itemBasedCFRatingDF.selectExpr("main_category", "recommended_categories", "similarity_scores", "rating_score"), "item_based_cf")  // 将基于物品的协同过滤评分写入 MySQL 的 "item_based_cf" 表

    // d. User-Based CF（使用余弦相似度）
    val userFeaturesDF = userValidDF.groupBy("user_id")  // 按用户 ID 分组
      .pivot("order_category")  // 将订单类别字段进行透视
      .agg(avg("rating"))  // 聚合计算每个用户每个类别的平均评分
      .na.fill(0)  // 用 0 填充空值

    val userVectorsDF = userFeaturesDF.select(  // 选择用户 ID 和对应的特征向量
      col("user_id"),
      array(userFeaturesDF.columns.filter(_ != "user_id").map(col): _*).cast("array<double>").alias("features")
    )

    val userSimilarityDF = userVectorsDF.alias("user1")  // 为用户特征数据框设置别名 "user1"
      .join(userVectorsDF.alias("user2"), col("user1.user_id") < col("user2.user_id"))  // 与自身连接，筛选用户对
      .select(
        col("user1.user_id").alias("user1"),  // 选择第一个用户的 ID，重命名为 "user1"
        col("user2.user_id").alias("user2"),  // 选择第二个用户的 ID，重命名为 "user2"
        cosineSimilarityUDF(col("user1.features"), col("user2.features")).alias("cosine_similarity")  // 计算两个用户特征向量的余弦相似度，重命名为 "cosine_similarity"
      )

    val userRecommendationsRatingDF = userSimilarityDF
      .join(  // 与用户评分数据连接
        userValidDF.select(
          col("user_id").alias("similar_user"),  // 选择用户 ID 字段，重命名为 "similar_user"
          col("order_category"),  // 选择订单类别字段
          col("rating")  // 选择评分字段
        ),
        expr("user2 = similar_user"),  // 连接条件：用户 2 等于相似用户
        "left_outer"  // 左外连接
      )
      .groupBy("user1", "order_category")  // 按用户 1 和订单类别分组
      .agg(
        avg("rating").alias("avg_rating")  // 聚合计算平均评分
      )
      .groupBy("user1")  // 按用户 1 分组
      .agg(
        concat_ws(",", collect_set("order_category")).alias("recommended_categories"),  // 聚合收集订单类别并用逗号连接，重命名为 "recommended_categories"
        concat_ws(",", collect_list(col("avg_rating").cast("string"))).alias("predicted_ratings")  // 聚合收集平均评分并用逗号连接，重命名为 "predicted_ratings"
      )
      .withColumnRenamed("user1", "user_id")  // 将用户 1 列重命名为 "user_id"
    println("\n===== user_based_cf 数据 =====")  // 打印分隔符和标题
    userRecommendationsRatingDF.show(truncate = false)  // 显示基于用户的协同过滤评分数据框内容，不截断
    writeDataToMySQL(userRecommendationsRatingDF.selectExpr("user_id", "recommended_categories", "predicted_ratings"), "user_based_cf")  // 将基于用户的协同过滤评分写入 MySQL 的 "user_based_cf" 表

    orderCategoryMatrixDF  // 返回订单-类别关系矩阵数据框
  }

  // 数据清洗
  def cleanData(df: DataFrame): DataFrame = {  // 定义清洗订单数据的方法
    df
      .withColumn("order_quantity", coalesce(col("order_quantity"), lit(0)))  // 用 0 填充订单数量字段的空值
      .withColumn("is_valid", coalesce(upper(col("is_valid")), lit("N")))  // 将是否有效字段转换为大写，并用 "N" 填充空值
      .withColumn("order_date", coalesce(to_timestamp(col("order_date"), "yyyy-MM-dd[ HH:mm:ss]"), current_timestamp()))  // 将订单日期字段转换为时间戳，并用当前时间戳填充空值
      .withColumn("order_category", coalesce(col("order_category"), lit("Unknown")))  // 用 "Unknown" 填充订单类别字段的空值
      .withColumn("order_name", coalesce(col("order_name"), lit("Unknown")))  // 用 "Unknown" 填充订单名称字段的空值
      .dropDuplicates("order_name", "order_category", "order_date")  // 去除重复的订单名称、订单类别和订单日期组合
  }

  def cleanUserRatingData(df: DataFrame): DataFrame = {  // 定义清洗用户评分数据的方法
    df
      .withColumn("order_quantity", coalesce(col("order_quantity"), lit(0)))  // 用 0 填充订单数量字段的空值
      .withColumn("is_valid", coalesce(upper(col("is_valid")), lit("N")))  // 将是否有效字段转换为大写，并用 "N" 填充空值
      .withColumn("rating", coalesce(col("rating"), lit(0)))  // 用 0 填充评分字段的空值
      .withColumn("order_category", coalesce(col("order_category"), lit("Unknown")))  // 用 "Unknown" 填充订单类别字段的空值
      .withColumn("order_name", coalesce(col("order_name"), lit("Unknown")))  // 用 "Unknown" 填充订单名称字段的空值
      .withColumn("user_id", coalesce(col("user_id"), lit(-1)))  // 用 -1 填充用户 ID 字段的空值
      .dropDuplicates("order_name", "user_id")  // 去除重复的订单名称和用户 ID 组合
  }

  // MySQL写入方法
  def writeDataToMySQL(df: DataFrame, tableName: String): Unit = {  // 定义将数据框写入 MySQL 表的方法
    val dataToWrite = df.collect()  // 将数据框转换为数组
    if (dataToWrite.nonEmpty) {  // 如果数组不为空
      val connection = DriverManager.getConnection(dbConfig("url"), dbConfig("user"), dbConfig("password"))  // 获取数据库连接
      try {
        val metaData = connection.getMetaData  // 获取数据库元数据
        val tableColumns = getTableColumns(metaData, tableName)  // 获取目标表的列名集合
        val columns = df.schema.fieldNames.filter(tableColumns.contains)  // 筛选数据框中与表列名匹配的列名

        val updateClause = columns.map { fieldName =>  // 构建更新子句
          if (tableName == "category_category_matrix" && Seq("association_count", "average_score").contains(fieldName)) {
            if (fieldName == "association_count") {
              s"$fieldName = $fieldName + VALUES($fieldName)"  // 对关联次数进行累加更新
            } else if (fieldName == "average_score") {
              s"$fieldName = ($fieldName + VALUES($fieldName)) / 2"  // 对平均相似度进行平均更新
            } else {
              s"$fieldName = VALUES($fieldName)"  // 普通字段直接更新为新值
            }
          } else {
            s"$fieldName = VALUES($fieldName)"  // 普通字段直接更新为新值
          }
        }.mkString(", ")

        val placeholders = columns.map(_ => "?").mkString(", ")  // 构建占位符字符串
        val sql = s"""
        INSERT INTO $tableName (${columns.mkString(", ")} )
        VALUES ($placeholders)
        ON DUPLICATE KEY UPDATE $updateClause
        """  // 构建插入 SQL 语句，包含重复键更新逻辑

        val pstmt = connection.prepareStatement(sql)  // 创建预编译 SQL 语句对象
        dataToWrite.foreach { row =>  // 遍历数据数组
          columns.zipWithIndex.foreach { case (colName, index) =>  // 遍历列名和索引
            pstmt.setObject(index + 1, row.getAs[Object](colName))  // 设置 SQL 语句参数值
          }
          pstmt.executeUpdate()  // 执行更新
        }
      } catch {
        case e: Exception =>  // 捕获异常
          logger.error(s"MySQL写入失败 [表: $tableName]", e)  // 记录错误日志
      } finally {
        if (connection != null) connection.close()  // 关闭数据库连接
      }
    }
  }

  def getTableColumns(metaData: java.sql.DatabaseMetaData, tableName: String): Set[String] = {  // 定义获取表列名集合的方法
    val rs = metaData.getColumns(null, null, tableName, null)  // 获取表的列信息
    var columns = Set[String]()  // 初始化列名集合
    while (rs.next()) {  // 遍历结果集
      columns += rs.getString("COLUMN_NAME")  // 添加列名到集合
    }
    rs.close()  // 关闭结果集
    columns  // 返回列名集合
  }
}