package orderProcessing

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import java.sql.{Connection, DriverManager, PreparedStatement}

object KafkaToMySQL {
  val logger = Logger.getLogger(getClass.getName)

  // 敏感配置一律从环境变量读取；缺失时直接失败，避免把账号口令硬编码进仓库
  private def requireEnv(name: String): String =
    Option(System.getenv(name)).getOrElse(
      throw new IllegalStateException(
        "缺少必需的环境变量 " + name + "，请参考 README「配置项（环境变量）」一节"
      )
    )

  // MySQL 配置（JDBC URL / 账号 / 密码均来自环境变量）
  val dbConfig = Map(
    "url" -> requireEnv("MYSQL_JDBC_URL"),
    "user" -> requireEnv("DB_USER"),
    "password" -> requireEnv("DB_PASSWORD"),
    "driver" -> "com.mysql.cj.jdbc.Driver"
  )

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("KafkaToMySQL")
      .setMaster("local[*]")
      .set("spark.sql.shuffle.partitions", "2")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val ssc = new StreamingContext(conf, Seconds(10))
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    // 注册余弦相似度UDF
    val cosineSimilarityUDF = udf((vec1: Seq[Double], vec2: Seq[Double]) => {
      if (vec1.isEmpty || vec2.isEmpty) 0.0
      else {
        val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum
        val normA = math.sqrt(vec1.map(x => x * x).sum)
        val normB = math.sqrt(vec2.map(x => x * x).sum)
        dotProduct / (normA * normB)
      }
    })

    // Kafka 消费者配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> requireEnv("KAFKA_BOOTSTRAP_SERVERS"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "niit",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("orders", "users")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // 定义数据结构
    val orderSchema = new StructType()
      .add("order_category", StringType)
      .add("order_name", StringType)
      .add("order_quantity", IntegerType)
      .add("order_date", StringType)
      .add("is_valid", StringType)

    val userSchema = new StructType(orderSchema.fields)
      .add("user_id", IntegerType)
      .add("rating", IntegerType)

    stream.foreachRDD { rdd =>
      try {
        if (!rdd.isEmpty()) {
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

          // 处理订单数据
          val df = rdd.map(record => record.value)
            .toDF("value")
            .withColumn("parsed_data", from_json(col("value"), orderSchema))
            .filter(col("parsed_data").isNotNull)
            .select("parsed_data.*")

          val cleanedDF = cleanData(df.cache())
          cleanedDF.createOrReplaceTempView("order_data")
          println("\n===== cleanedDF 数据 =====")
          cleanedDF.show(truncate = false)
          writeDataToMySQL(cleanedDF.select(
            col("order_category"),
            col("order_name"),
            col("order_quantity"),
            col("order_date"),
            col("is_valid")
          ), "orders")

          if (!cleanedDF.isEmpty) {
            cleanedDF.printSchema()
            cleanedDF.show(5, truncate = false)
          }

          // 处理用户数据
          val userDF = rdd.map(record => record.value)
            .toDF("value")
            .withColumn("parsed_data", from_json(col("value"), userSchema))
            .filter(col("parsed_data").isNotNull)
            .select("parsed_data.*")

          val userCleanedDF = cleanUserRatingData(userDF.cache())
          println("\n===== userCleanedDF 数据 =====")
          userCleanedDF.show(truncate = false)
          writeDataToMySQL(userCleanedDF, "users")

          // 原有统计功能
          val totalOrdersDF = executeOriginalStatistics(spark, cleanedDF)
          println("\n===== totalOrdersDF 数据 =====")
          totalOrdersDF.show(truncate = false)

          // 推荐功能
          val orderCategoryMatrixDF = executeRecommendations(spark, cleanedDF, userCleanedDF, cosineSimilarityUDF)
          println("\n===== orderCategoryMatrixDF 数据 =====")
          orderCategoryMatrixDF.show(truncate = false)

          cleanedDF.unpersist()
          userCleanedDF.unpersist()
          stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
      } catch {
        case e: Exception =>
          logger.error("Stream processing failed", e)
          ssc.stop(stopSparkContext = true, stopGracefully = true)
          spark.stop()
      }
    }

    sys.addShutdownHook {
      ssc.stop(stopSparkContext = true, stopGracefully = true)
      spark.stop()
    }

    ssc.start()
    ssc.awaitTerminationOrTimeout(12000000)
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    spark.stop()
  }

  // 统计功能封装
  private def executeOriginalStatistics(spark: SparkSession, cleanedDF: DataFrame): DataFrame = {
    import spark.implicits._

    // 1. 实时统计所有有效和无效订单号的总和
    val totalOrdersDF = cleanedDF.groupBy("is_valid")
      .agg(count("*").alias("total_count"))
    println("\n===== total_orders 数据 =====")
    totalOrdersDF.show(truncate = false)
    writeDataToMySQL(totalOrdersDF, "total_orders")

    // 2. 实时统计各个订单号各自的有效和无效数量
    val orderStatsDF = cleanedDF.groupBy("order_name", "is_valid")
      .agg(
        count("*").alias("count_per_order"),
        sum("order_quantity").alias("total_quantity")
      )
    println("\n===== order_stats 数据 =====")
    orderStatsDF.show(truncate = false)
    writeDataToMySQL(orderStatsDF, "order_stats")

    // 3. 实时统计所有订单类别的数量
    val categoryStatsDF = cleanedDF.groupBy("order_category")
      .agg(
        count("*").alias("category_count"),
        avg("order_quantity").alias("avg_quantity")
      )
    println("\n===== category_stats 数据 =====")
    categoryStatsDF.show(truncate = false)
    writeDataToMySQL(categoryStatsDF, "category_stats")

    // 4. 使用 Spark SQL 统计
    val sqlStatsDF = spark.sql(
      """SELECT order_name,
        |SUM(CASE WHEN is_valid = 'Y' THEN 1 ELSE 0 END) AS valid_count,
        |SUM(CASE WHEN is_valid = 'N' THEN 1 ELSE 0 END) AS invalid_count
        |FROM order_data
        |GROUP BY order_name""".stripMargin
    )
    println("\n===== sql_stats 数据 =====")
    sqlStatsDF.show(truncate = false)
    writeDataToMySQL(sqlStatsDF, "sql_stats")

    // 5. 使用 Spark Core/RDD 统计
    val rddStatsDF = cleanedDF.rdd
      .map(row => (
        row.getString(1),  // order_name
        row.getString(0),  // order_category
        row.getString(4)  // is_valid
      ))
      .map(t => ((t._1, t._2, t._3), 1))
      .reduceByKey(_ + _)
      .map { case ((name, category, valid), count) =>
        (name, category, valid, count)
      }
      .toDF("order_name", "order_category", "is_valid", "count")
    println("\n===== rdd_stats 数据 =====")
    rddStatsDF.show(truncate = false)
    writeDataToMySQL(rddStatsDF, "rdd_stats")

    totalOrdersDF
  }

  // 推荐功能封装
  private def executeRecommendations(spark: SparkSession,
                                     cleanedDF: DataFrame,
                                     userCleanedDF: DataFrame,
                                     cosineSimilarityUDF: UserDefinedFunction): DataFrame = {
    import spark.implicits._

    val validDF = cleanedDF.filter(col("is_valid") === "Y")
    val userValidDF = userCleanedDF.filter(col("is_valid") === "Y")

    // a. 订单-类别关系矩阵（累加类别在订单中的出现次数）
    val orderCategoryMatrixDF = validDF
      .select("order_name", "order_category")
      .groupBy("order_name", "order_category")
      .agg(count("*").alias("category_count"))
      .groupBy("order_name")
      .pivot("order_category")
      .agg(sum("category_count").alias("total_count"))
      .na.fill(0)
    println("\n===== order_category_matrix 数据 =====")
    orderCategoryMatrixDF.show(truncate = false)
    writeDataToMySQL(orderCategoryMatrixDF, "order_category_matrix")

    // b. 类别-类别 关系矩阵（使用余弦相似度）
    val categoryItemMatrix = validDF
      .select("order_name", "order_category")
      .distinct()
      .groupBy("order_category")
      .pivot("order_name")
      .agg(count("*"))
      .na.fill(0)

    val categoryVectorsDF = categoryItemMatrix.select(
      col("order_category"),
      array(categoryItemMatrix.columns.filter(_ != "order_category").map(col): _*).cast("array<double>").alias("features")
    )

    val categorySimilarityDF = categoryVectorsDF.alias("cat1")
      .join(categoryVectorsDF.alias("cat2"), col("cat1.order_category") < col("cat2.order_category"))
      .select(
        col("cat1.order_category").alias("category_a"),
        col("cat2.order_category").alias("category_b"),
        cosineSimilarityUDF(col("cat1.features"), col("cat2.features")).alias("cosine_similarity")
      )
      .groupBy("category_a", "category_b")
      .agg(avg("cosine_similarity").alias("average_score"))

    // 计算 association_count 和平均 average_score
    val categoryPairsDF = categorySimilarityDF.groupBy("category_a", "category_b")
      .agg(
        count("*").alias("association_count"),
        avg("average_score").alias("average_score")
      )

    println("\n===== category_category_matrix 数据 =====")
    categoryPairsDF.show(truncate = false)
    writeDataToMySQL(categoryPairsDF, "category_category_matrix")

    // c. Item-Based CF（使用余弦相似度）
    val categoryFeaturesDF = validDF.groupBy("order_category")
      .pivot("order_name")
      .agg(avg("order_quantity"))
      .na.fill(0)

    val categoryVectorsDFItem = categoryFeaturesDF.select(
      col("order_category"),
      array(categoryFeaturesDF.columns.filter(_ != "order_category").map(col): _*).cast("array<double>").alias("features")
    )

    val categorySimilarityDFItem = categoryVectorsDFItem.alias("cat1")
      .join(categoryVectorsDFItem.alias("cat2"), col("cat1.order_category") < col("cat2.order_category"))
      .select(
        col("cat1.order_category").alias("category_a"),
        col("cat2.order_category").alias("category_b"),
        cosineSimilarityUDF(col("cat1.features"), col("cat2.features")).alias("cosine_similarity")
      )

    val itemBasedCFRatingDF = categorySimilarityDFItem
      .join(
        userValidDF.select(
          col("order_category").alias("category"),
          col("user_id"),
          col("rating")
        ),
        expr("category_a = category"),
        "left_outer"
      )
      .groupBy("category_a")
      .agg(
        concat_ws(",", collect_set("category_b")).alias("recommended_categories"),
        concat_ws(",", collect_list(col("cosine_similarity").cast("string"))).alias("similarity_scores"),
        avg("rating").cast("int").alias("rating_score")
      )
      .withColumnRenamed("category_a", "main_category")
    println("\n===== item_based_cf 数据 =====")
    itemBasedCFRatingDF.show(truncate = false)
    writeDataToMySQL(itemBasedCFRatingDF.selectExpr("main_category", "recommended_categories", "similarity_scores", "rating_score"), "item_based_cf")

    // d. User-Based CF（使用余弦相似度）
    val userFeaturesDF = userValidDF.groupBy("user_id")
      .pivot("order_category")
      .agg(avg("rating"))
      .na.fill(0)

    val userVectorsDF = userFeaturesDF.select(
      col("user_id"),
      array(userFeaturesDF.columns.filter(_ != "user_id").map(col): _*).cast("array<double>").alias("features")
    )

    val userSimilarityDF = userVectorsDF.alias("user1")
      .join(userVectorsDF.alias("user2"), col("user1.user_id") < col("user2.user_id"))
      .select(
        col("user1.user_id").alias("user1"),
        col("user2.user_id").alias("user2"),
        cosineSimilarityUDF(col("user1.features"), col("user2.features")).alias("cosine_similarity")
      )

    val userRecommendationsRatingDF = userSimilarityDF
      .join(
        userValidDF.select(
          col("user_id").alias("similar_user"),
          col("order_category"),
          col("rating")
        ),
        expr("user2 = similar_user"),
        "left_outer"
      )
      .groupBy("user1", "order_category")
      .agg(
        avg("rating").alias("avg_rating")
      )
      .groupBy("user1")
      .agg(
        concat_ws(",", collect_set("order_category")).alias("recommended_categories"),
        concat_ws(",", collect_list(col("avg_rating").cast("string"))).alias("predicted_ratings")
      )
      .withColumnRenamed("user1", "user_id")
    println("\n===== user_based_cf 数据 =====")
    userRecommendationsRatingDF.show(truncate = false)
    writeDataToMySQL(userRecommendationsRatingDF.selectExpr("user_id", "recommended_categories", "predicted_ratings"), "user_based_cf")

    orderCategoryMatrixDF
  }

  // 数据清洗方法（保持原有逻辑）
  def cleanData(df: DataFrame): DataFrame = {
    df
      .withColumn("order_quantity", coalesce(col("order_quantity"), lit(0)))
      .withColumn("is_valid", coalesce(upper(col("is_valid")), lit("N")))
      .withColumn("order_date", coalesce(to_timestamp(col("order_date"), "yyyy-MM-dd[ HH:mm:ss]"), current_timestamp()))
      .withColumn("order_category", coalesce(col("order_category"), lit("Unknown")))
      .withColumn("order_name", coalesce(col("order_name"), lit("Unknown")))
      .dropDuplicates("order_name", "order_category", "order_date")
  }

  def cleanUserRatingData(df: DataFrame): DataFrame = {
    df
      .withColumn("order_quantity", coalesce(col("order_quantity"), lit(0)))
      .withColumn("is_valid", coalesce(upper(col("is_valid")), lit("N")))
      .withColumn("rating", coalesce(col("rating"), lit(0)))
      .withColumn("order_category", coalesce(col("order_category"), lit("Unknown")))
      .withColumn("order_name", coalesce(col("order_name"), lit("Unknown")))
      .withColumn("user_id", coalesce(col("user_id"), lit(-1)))
      .dropDuplicates("order_name", "user_id")
  }

  // MySQL写入方法（保持原有逻辑）
  def writeDataToMySQL(df: DataFrame, tableName: String): Unit = {
    val dataToWrite = df.collect()
    if (dataToWrite.nonEmpty) {
      val connection = DriverManager.getConnection(dbConfig("url"), dbConfig("user"), dbConfig("password"))
      try {
        val metaData = connection.getMetaData
        val tableColumns = getTableColumns(metaData, tableName)
        val columns = df.schema.fieldNames.filter(tableColumns.contains)

        val updateClause = columns.map { fieldName =>
          if (tableName == "category_category_matrix" && Seq("association_count", "average_score").contains(fieldName)) {
            if (fieldName == "association_count") {
              s"$fieldName = $fieldName + VALUES($fieldName)" // 累加关联次数
            } else if (fieldName == "average_score") {
              s"$fieldName = ($fieldName + VALUES($fieldName)) / 2" // 计算平均值
            } else {
              s"$fieldName = VALUES($fieldName)"
            }
          } else {
            s"$fieldName = VALUES($fieldName)"
          }
        }.mkString(", ")

        val placeholders = columns.map(_ => "?").mkString(", ")
        val sql = s"""
        INSERT INTO $tableName (${columns.mkString(", ")} )
        VALUES ($placeholders)
        ON DUPLICATE KEY UPDATE $updateClause
        """

        val pstmt = connection.prepareStatement(sql)
        dataToWrite.foreach { row =>
          columns.zipWithIndex.foreach { case (colName, index) =>
            pstmt.setObject(index + 1, row.getAs[Object](colName))
          }
          pstmt.executeUpdate()
        }
      } catch {
        case e: Exception =>
          logger.error(s"MySQL写入失败 [表: $tableName]", e)
      } finally {
        if (connection != null) connection.close()
      }
    }
  }

  def getTableColumns(metaData: java.sql.DatabaseMetaData, tableName: String): Set[String] = {
    val rs = metaData.getColumns(null, null, tableName, null)
    var columns = Set[String]()
    while (rs.next()) {
      columns += rs.getString("COLUMN_NAME")
    }
    rs.close()
    columns
  }
}