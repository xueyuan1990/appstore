package com.test

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import linj.newscluster.{ImprovedTextRanker, RankConf}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linj.Word2Vec
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.immutable.HashMap
import scala.util.matching.Regex

/**
  * Created by xueyuan on 2017/5/25.
  */
object app_sim_keywords {
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val sdf: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val userName = "mzsip"
    System.setProperty("user.name", userName)
    System.setProperty("HADOOP_USER_NAME", userName)
    println("***********************start*****************************")
    val sparkConf: SparkConf = new SparkConf().setAppName("app_store_training_xueyuan")
    sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    println("***********************sc*****************************")
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    println("***********************hive*****************************")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //[window_size , vector_size,learning_rate , min_count,num_iterations, num_partitions , seed,save_n,partition_num,min_app_num,table_in,table_out,table_temp,date2,date]
    var window_size = 0
    var vector_size = 0
    var learning_rate = 0.0
    var min_count = 0
    var num_iterations = 0
    var num_partitions = 0
    var seed = 0L
    var save_n = 0
    var partition_num = 1
    //    var min_app_num = 0
    var table_in = ""
    var table_out = ""
    var table_temp = ""
    var date = ""
    var date2 = ""
    if (args.length == 15) {
      window_size = args(0).toInt
      println("***********************window_size=" + window_size + "*****************************")
      vector_size = args(1).toInt
      println("***********************vector_size=" + vector_size + "*****************************")
      learning_rate = args(2).toDouble
      println("***********************learning_rate=" + learning_rate + "*****************************")
      min_count = args(3).toInt
      println("***********************min_count=" + min_count + "*****************************")
      num_iterations = args(4).toInt
      println("***********************num_iterations=" + num_iterations + "*****************************")
      num_partitions = args(5).toInt
      println("***********************num_partitions=" + num_partitions + "*****************************")
      seed = args(6).toLong
      println("***********************seed=" + seed + "*****************************")
      save_n = args(7).toInt
      println("***********************save_n=" + save_n + "*****************************")
      partition_num = args(8).toInt
      println("***********************partition_num=" + partition_num + "*****************************")
      //      min_app_num = args(9).toInt
      //      println("***********************min_app_num=" + min_app_num + "*****************************")
      table_in = args(10).toString
      println("***********************table_in=" + table_in + "*****************************")
      table_out = args(11).toString
      println("***********************table_out=" + table_out + "*****************************")
      table_temp = args(12).toString
      println("***********************table_temp=" + table_temp + "*****************************")
      date2 = args(13).toString
      println("***********************date2=" + date2 + "*****************************")
      date = args(14).toString
      println("***********************date=" + date + "*****************************")
    }

    //load
    //    val sql_search = "select pak_id,pos_text from algo.tlc_jd_hotapps_relate_suggest_treetagger"
    val sql_search = "select b.app_name,a.pos_text from algo.tlc_jd_hotapps_relate_suggest_treetagger as a, uxip.ads_rpt_uxip_hotapps_relate_suggest_d as b where a.pak_id=b.pak_id and b.stat_date=20170525"
    val app_word_wordnn = load_app(sql_search)
    //    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************app_word_wordnn.size="+app_word_wordnn.count()+"*****************************")
    val app_wordnn = app_word_wordnn.map(r => (r._1, r._3))
    //caculate idf
    val wordIdf = get_idf(app_wordnn)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************idf finished*****************************")
    //find key words
    val rankConf = new RankConf
    val title = new Array[String](0)
    val wordVec = new HashMap[String, Array[Float]]()
    val app_keywords = app_wordnn.mapPartitions(iter => {
      var app_keyword = new ArrayBuffer[(String, Array[String])]()
      for ((app, word) <- iter) {
        val content = new Array[Array[String]](1)
        content(0) = word
        val improvedTextRanker = new ImprovedTextRanker(rankConf, title, content, wordVec, wordIdf)
        val keyword_idf = improvedTextRanker.calcScore()
        app_keyword += ((app, keyword_idf.toMap.keySet.toArray))
      }
      app_keyword.iterator
    })

    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************keyword finished*****************************")
    //word2vector

    val word2vector = new Word2Vec()
    word2vector.setWindowSize(window_size)
    word2vector.setVectorSize(vector_size)
    if (learning_rate > 0) {
      word2vector.setLearningRate(learning_rate)
    }
    //最小出现次数
    if (min_count >= 0) {
      word2vector.setMinCount(min_count)
    }
    if (num_iterations > 0) {
      word2vector.setNumIterations(num_iterations)
    }
    if (num_partitions > 0) {
      word2vector.setNumPartitions(num_partitions)
    }
    if (seed > 0) {
      word2vector.setSeed(seed)
    }
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************word2vector start*****************************")
    val app_word = app_word_wordnn.map(r => r._2)
    //    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************app_word.size=" + app_word.count() + "*****************************")
    val word_vector = word2vector.fit(app_word).toMap
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************word2vector finished*****************************")
    val exmp = word_vector.take(10)
    for (e <- exmp) {
      val vector = e._2
      print(e._1 + ": ")
      var sum = 0.0
      for (v <- vector) {
        sum += (v * v)
        print(v + ", ")
      }
      println("***********************" + sum + "*****************************")
    }

    //get key word vector
    val vector_size_br = sc.broadcast(vector_size)
    val app_vector = app_keywords.map(r => {
      var vector = new Array[Float](vector_size_br.value)
      for (keyword <- r._2) {
        val v = word_vector(keyword).zip(vector)
        vector = v.map(r => r._1 + r._2)
      }
      (r._1, vector)
    })
    //find sim app
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************repartition start*****************************")
    app_vector.repartition(partition_num)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************repartition finished*****************************")
    val av = app_vector.collect()
    val app_vector_br = sc.broadcast(av)

    def find_sim(iterator: Iterator[(String, Array[Float])]): Iterator[(String, Array[(String, Double)])] = {
      var result: ArrayBuffer[(String, Array[(String, Double)])] = new ArrayBuffer[(String, Array[(String, Double)])]()
      val word2vector_total = app_vector_br.value
      while (iterator.hasNext) {
        var sim_array: ArrayBuffer[(String, Double)] = new ArrayBuffer[(String, Double)]()
        val r = iterator.next()
        for ((w, v) <- word2vector_total if !w.equals(r._1)) {
          sim_array += ((w, sim(r._2, v)))
        }
        val sim_apps = sim_array.sortWith(_._2 > _._2).take(save_n).toArray
        result += ((r._1, sim_apps))
      }
      result.iterator
    }

    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************find sim app start*****************************")
    val sim_app_rdd = app_vector.mapPartitions(find_sim)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************find sim app finished*****************************")
    save_data(table_out, table_temp, sim_app_rdd)


  }

  def get_idf(app_word: RDD[(String, Array[String])]): Map[String, Float] = {
    val total_size = app_word.count()
    val word_set = app_word.flatMap(r => r._2).map(r => (r, 1)).reduceByKey(_ + _).map(r => r._1)
    val aw = app_word.collect()
    val app_word_br = sc.broadcast(aw)
    val total_size_br = sc.broadcast(total_size)
    val idf = word_set.mapPartitions(iter => {
      val app_word_value = app_word_br.value
      val total_size_value = total_size_br.value
      var word_idf: Map[String, Float] = new HashMap[String, Float]()
      for (item <- iter) {
        var count = 0
        for ((a, w) <- app_word_value) {
          if (w.contains(item)) {
            count += 1
          }
        }
        val idf = math.log(total_size_value / (1.0 + count))
        word_idf += (item -> idf.toFloat)
      }
      word_idf.iterator
    })
    idf.collect().toMap
  }

  def load_app(sql_search: String): RDD[(String, Iterable[String], Array[String])] = {
    val data = hiveContext.sql(sql_search).map(r => (r.getString(0), r.getString(1).split(" ")))
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************load finished *****************************")
    val app_word_wordnn = data.map(r => {
      var words: ArrayBuffer[String] = new ArrayBuffer[String]()
      var words_nn: ArrayBuffer[String] = new ArrayBuffer[String]()
      //      val pattern = new Regex("_(NN|NNS)$")
      for (item <- r._2) {
        val w_f = item.split("_")
        if (w_f.length == 2) {
          words += w_f(0)
          if ("NN".equals(w_f(1)) || "NNS".equals(w_f(1))) {
            words_nn += w_f(0)
          }
        }


      }
      (r._1, words.toArray.toIterable, words_nn.toArray)
    })
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************get word nn finished*****************************")
    app_word_wordnn
  }

  def save_data(table_out: String, table_temp: String, sim_app_rdd: RDD[(String, Array[(String, Double)])]): Unit = {

    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************save table start*****************************")
    val data = sim_app_rdd.zipWithIndex().map(r => {
      var app_sim_string = ""
      for ((app, sim) <- r._1._2) {
        app_sim_string += (app + ":" + sim + ", ")
      }
      (r._2, r._1._1, app_sim_string)

    })
    val candidate_rdd = data.map(r => Row(r._1, r._2, r._3))

    val structType = StructType(
      StructField("id", LongType, false) ::
        StructField("app_package", StringType, false) ::
        StructField("sim_apps", StringType, false) ::
        Nil
    )

    //from RDD to DataFrame
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    val create_table_sql: String = "create table if not exists " + table_out + " (id BIGINT, app_package String, sim_apps String) partitioned by (stat_date bigint) stored as textfile"
    val c1 = Calendar.getInstance()
    c1.add(Calendar.DATE, -1)
    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    val date1 = sdf1.format(c1.getTime())
    val insertInto_table_sql: String = "insert overwrite table " + table_out + " partition(stat_date = " + date1 + ") select * from "
    //    val insertInto_table_sql: String = "insert overwrite table " + table_out + "  select * from "
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************save data start*****************************")
    candidate_df.registerTempTable(table_temp)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************register TempTable finished*****************************")
    hiveContext.sql(create_table_sql)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************create table finished*****************************")
    hiveContext.sql(insertInto_table_sql + table_temp)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************insertInto table finished*****************************")
  }


  def sim(word1: Array[Float], word2: Array[Float]): Double = {
    val member = word1.zip(word2).map(d => d._1 * d._2).reduce(_ + _)
    //求出分母第一个变量值
    val temp1 = math.sqrt(word1.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母第二个变量值
    val temp2 = math.sqrt(word2.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母
    val denominator = temp1 * temp2
    val sim = member / denominator
    sim
  }
}
