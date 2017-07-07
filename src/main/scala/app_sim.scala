package com.test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linj.Word2Vec
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * Created by xueyuan on 2017/5/25.
  */
object app_sim {
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
    val sql_search_outer_store = "select imei as umid, b.app_name as app_name, a.oper_time from uxip.dwd_uxip_hotapps_operate_detail_d as a, uxip.ads_rpt_uxip_hotapps_relate_suggest_d as b where " +
      "a.pak_id=b.pak_id and a.oper_event=3 and (b.app_category_id<14 or b.app_category_id>31) and a.stat_date<=" + date + " and b.stat_date=" + date
    val sql_search_outer_game = "select imei as umid, b.app_name as app_name, a.oper_time from uxip.dwd_uxip_hotapps_operate_detail_d as a, uxip.ads_rpt_uxip_hotapps_relate_suggest_d as b where " +
      "a.pak_id=b.pak_id and a.oper_event=3 and (b.app_category_id>=14 and b.app_category_id<=31) and a.stat_date<=" + date + " and b.stat_date=" + date
    val sql_search_inner_store = "select a.umid, b.fname, a.event_tm from uxip.edl_uxip_event_detail_di as a, app_center.ods_t_app_application_d as b, " +
      "app_center.idl_fdt_packageid_appid_mapping as c where a.stat_date<=" + date + " and a.stat_date>=" + date2 + " and b.stat_date=" + date + " and c.stat_date=" + date +
      " and a.misc_map['appid']=b.fid and b.fid=c.appid and b.fcategoryid=1 and c.fstatus=1 and c.packageid is not NULL and a.event_name='install_status' and misc_map['status'] = 1 and misc_map ['type'] = 1 and a.app_name='com.meizu.mstore' " +
      " and regexp_replace(imei, '0', '') > 0 and length(imei) = 15"
    val sql_search_inner_game = "select a.umid, b.fname, a.event_tm from uxip.edl_uxip_event_detail_di as a, app_center.ods_t_app_application_d as b, " +
      "app_center.idl_fdt_packageid_appid_mapping as c where a.stat_date<=" + date + " and a.stat_date>=" + date2 + " and b.stat_date=" + date + " and c.stat_date=" + date +
      " and a.misc_map['appid']=b.fid and b.fid=c.appid and b.fcategoryid=2 and c.fstatus=1 and c.packageid is not NULL and a.event_name='install_status' and misc_map['status'] = 1 and misc_map ['type'] = 1 and (a.app_name='com.meizu.mstore' or a.app_name='com.meizu.flyme.gamecenter') " +
      " and regexp_replace(imei, '0', '') > 0 and length(imei) = 15"
    var sql_search = ""
    if ("outer_store".equals(table_in)) {
      sql_search = sql_search_outer_store
    } else if ("inner_store".equals(table_in)) {
      sql_search = sql_search_inner_store
    } else if ("inner_game".equals(table_in)) {
      sql_search = sql_search_inner_game
    } else if ("outer_game".equals(table_in)) {
      sql_search = sql_search_outer_game
    }
    val app_sorted = load_app(sql_search)
    //word2vector
    val word2vector = new Word2Vec()
    word2vector.setWindowSize(window_size)
    word2vector.setVectorSize(vector_size)
    if (learning_rate > 0) {
      word2vector.setLearningRate(learning_rate)
    }
    //最小出现次数
    if (min_count > 0) {
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
    val word2vector_result = word2vector.fit(app_sorted)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************word2vector finished*****************************")
    val exmp = word2vector_result.take(10)
    for (e <- exmp) {
      val vector = e._2
      print(e._1 + ": ")
      var sum = 0.0
      for (v <- vector) {
        sum += v
        print(v + ", ")
      }
      println("***********************" + sum + "*****************************")
    }

    //find sim app
    var word2vector_rdd = sc.parallelize(word2vector_result)
    if (save_n > 0) {
      word2vector_rdd = sc.parallelize(word2vector_rdd.take(save_n))
    }
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************repartition start*****************************")
    word2vector_rdd.repartition(partition_num)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************repartition finished*****************************")
    val word2vector_total_br = sc.broadcast(word2vector_result)

    def find_sim(iterator: Iterator[(String, Array[Float])]): Iterator[(String, Array[(String, Double)])] = {
      var result: ArrayBuffer[(String, Array[(String, Double)])] = new ArrayBuffer[(String, Array[(String, Double)])]()
      val word2vector_total = word2vector_total_br.value
      while (iterator.hasNext) {
        var sim_array: ArrayBuffer[(String, Double)] = new ArrayBuffer[(String, Double)]()
        val r = iterator.next()
        for ((w, v) <- word2vector_total if !w.equals(r._1)) {
          sim_array += ((w, sim(r._2, v)))
        }
        val sim_apps = sim_array.sortWith(_._2 > _._2).take(10).toArray
        result += ((r._1, sim_apps))
      }
      result.iterator
    }

    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************find sim app start*****************************")
    val sim_app_rdd = word2vector_rdd.mapPartitions(find_sim)
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************find sim app finished*****************************")
    save_data(table_out, table_temp, sim_app_rdd)


  }

  def load_app(sql_search: String): RDD[Iterable[String]] = {
    //很快
    val user_app_time_rdd = hiveContext.sql(sql_search).map(r => (r.getString(0), r.getString(1), r.getString(2)))
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************load finished*****************************")
    //    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************user_app_time_rdd.size=" + user_app_time_rdd.count() + "*****************************")//耗时
    //drop apps less than min_app_num
    //    val min_app_num_br = sc.broadcast(min_app_num)
    //    val app_condidate = user_app_time_rdd.map(r => (r._2, 1)).reduceByKey(_ + _).filter(r => r._2 > min_app_num_br.value).map(r => r._1).toArray()

    //    val user_app_time = user_app_time_rdd.filter(r => app_condidate.contains(r._2)).map(r => (r._1, Array((r._2, r._3))))
    val user_app_time = user_app_time_rdd.map(r => (r._1, Array((r._2, r._3))))
    val app_time = user_app_time.reduceByKey(_ ++ _).map(r => r._2)
    val app_time_sorted = app_time.map(r => r.sortWith(_._2 < _._2))
    val app_sorted = app_time_sorted.map(r => {
      val array = r
      var apps: ArrayBuffer[String] = new ArrayBuffer[String]()
      for ((app, time) <- array) {
        apps += app
      }
      apps.toArray.toIterable
    })
    println(sdf.format(new Date((System.currentTimeMillis()))) + "***********************sort app by time finished*****************************")
    app_sorted
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
