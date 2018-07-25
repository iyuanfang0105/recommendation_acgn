package items_repository

import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.My_Utils
import config.Configure

import scala.collection.mutable.ArrayBuffer

object Dataset {
  def get_banned_authors(sparkSession: SparkSession, author_status_table: String) = {
    val select_author_sql = "select author_id, author_status, update_time from " + author_status_table
    val author_df = sparkSession.sql(select_author_sql).filter("author_id is not null and author_status is not null and update_time is not null")
    val author_rdd = author_df.rdd.map(v => (v.getString(0).trim, (v.getString(1).toInt, v.getString(2).toLong))).reduceByKey((a, b) => if (a._2 >= b._2) (a._1, a._2) else (b._1, b._2))
    val banned_authors = author_rdd.filter(v => v._2._1 != 1).map(_._1).distinct().collect() ++ Configure.authors("general_delete_authons")
    printf("\n====>>>> banned authors: %s\n", banned_authors.mkString(","))
    banned_authors
  }

  def get_basement_items_info(sparkSession: SparkSession, item_info_table: String, date_start: String, time_now: String) = {
    import sparkSession.implicits._

    val banned_authors = get_banned_authors(sparkSession, Configure.tables("author_status"))
    val select_item_sql = "select content_id, type, author_id, author as author_name, channel, publish_time, update_time, tag, status from " + item_info_table + " where stat_date>=" + date_start + " and status == 1"
    My_Utils.print_sql_str(select_item_sql)

    val item_info_df = sparkSession.sql(select_item_sql).filter("content_id is not null and publish_time is not null and channel is not null")

    val item_info_rdd = item_info_df.rdd.map(v => (v(0).toString.trim, v(1).toString, v(2).toString, v(3).toString, v(4).toString.trim, My_Utils.time_str_format(v(5).toString), My_Utils.time_str_format(v(6).toString), v(7).toString, v(8).toString)).filter(v => v._6.length >= 10 && v._7.length >= 10).map(v => (v._1, (v._2, v._3, v._4, v._5, v._6, v._7, v._8, v._9))).reduceByKey((a, b) => if (a._5 >= b._5) a else b).filter(v => !banned_authors.contains(v._2._2) && v._2._5 <= time_now)

    My_Utils.print_count(item_info_rdd.toDF(), "raw item info")

    val item_info_refined = item_info_rdd.filter(v => !contains_banned_tags(v._2._7))
    My_Utils.print_count(item_info_refined.toDF(), "refined item info(filter banned tags)")

    item_info_refined.map(v => (v._1, v._2._1, v._2._2, v._2._3, v._2._4, v._2._5, v._2._6, v._2._7, v._2._8))
  }

  def get_basement_actions(sparkSession: SparkSession, action_table: String, job_date: String, debug: Boolean): DataFrame = {
    import sparkSession.implicits._

    val select_sql: String = "select platform['_umid_'] as imei, event_values ['algoVersion'] as algo_version, event_values['content_id'] as content_id, event_infos['_name_'] as event_name, event_values from " + action_table + " where stat_date like \"" + job_date + "%\" and pkg_ver >= \"1.0.7\" and platform['_umid_'] is not null and event_values['content_id'] is not null and event_values['content_id'] != 0"

    // val select_sql: String = "select umid, imei, content_id, event_name, misc_map from " + "mzacgn.dwd_app_acgn_action_detail" + " where stat_hour like \"" + job_date + "%\" and imei != 0 and content_id != 0"

    printf("\n====>>>> %s\n", select_sql)
    val data = sparkSession.sql(select_sql).rdd.map(v => (v.getString(0), v.getString(1), v.getString(2), v.getString(3), v.getMap(4)))
    // My_Utils.print_count(data, "raw action")

    val data_refined = data.map(v => {
      val event_values = v._5.map(v => (v._1.toString, v._2.toString))
      var value: String = "null"
      if (Configure.events.contains(v._4)) {
        val temp = Configure.events(v._4)
        if (temp._2 == "1")
          value = event_values(temp._1) + ":" + temp._2
        else if (event_values.contains(temp._2))
          value = event_values(temp._1) + ":" + event_values(temp._2)
      }
      (v._1, v._2, v._3, v._4, value)
    }).map(v => (v._1, if (v._2 == "" || v._2 == "null" || v._2 == null) "unk" else v._2, v._3, v._4, v._5))

    if (debug) {
      val data_refined_count = data_refined.count()
      printf("\n====>>>> data_refined_count: %d", data_refined_count)
      data_refined.map(v => (v._2, 1)).reduceByKey(_+_).collect().foreach(v => printf("\n====>>>> %s : %d, %.4f", v._1, v._2, v._2 * 1.0 / data_refined_count))
    }

    data_refined.toDF()
  }

  def get_report_from_actions(sparkSession: SparkSession, action_table: String, algos: Array[String], job_date: String, debug: Boolean) = {

    val select_sql = "SELECT imei, algo_v, actions from " + action_table + " where stat_date=" + job_date + " and algo_v in (" + algos.mkString(", ") + ")"
    printf("\n====>>>> %s", select_sql)

    val data = sparkSession.sql(select_sql).rdd.map(v => (v.getString(0), v.getString(1), v.getString(2)))

    if (debug) {
      val data_count = data.count()
      printf("\n====>>>> actions of %s: %d", job_date, data_count)
      data.map(v => (v._3, 1)).reduceByKey(_+_).collect().foreach(v => printf("\n====>>>> %s: %d", v._1, v._2))
    }

    var report = ArrayBuffer[(String, Double, Double)]()
    for (algo <- algos) {
      val exposure_count = data.filter(v => v._2 == algo && (v._3 == "content_exposure" || v._3 == "channel_item_exposure")).count()
      val exposure_device_count = data.filter(v => v._2 == algo && (v._3 == "content_exposure" || v._3 == "channel_item_exposure")).map(_._1).distinct().count()

      val click_count = data.filter(v => v._2 == algo && (v._3 == "content_click")).count()
      val click_device_count = data.filter(v => v._2 == algo && (v._3 == "content_click")).map(_._1).distinct().count()

      val pv_ctr = click_count * 1.0 / exposure_count
      val uv_ctr = click_device_count * 1.0 / exposure_device_count
      report += ((algo, pv_ctr, uv_ctr))
    }

    if (debug) {
      report.foreach(v => printf("\n====>>>> algo: %s, pv_ctr: %.4f, uv_ctr: %.4f", v._1, v._2, v._3))
    }
    report
  }

  def get_item_statistic_crt(sparkSession: SparkSession, action_table: String, job_date: String) = {
    val select_sql: String = "SELECT * from " + action_table + " where stat_date = " + job_date
    val data_df = sparkSession.sql(select_sql).filter("imei is not null")
    My_Utils.print_count(data_df, "")
  }

  def contains_banned_tags(ss: String): Boolean = {
    val tag_info_arr = ss.split(",")
    val tag_name_bf: ArrayBuffer[String] = new ArrayBuffer[String]()
    val tag_id_bf: ArrayBuffer[String] = new ArrayBuffer[String]()
    val tag_code_bf: ArrayBuffer[String] = new ArrayBuffer[String]()
    for (item <- tag_info_arr) {
      val temp = item.split(":")
      if (temp.length == 3) {
        tag_code_bf += temp(0) + "#" + temp(1)
        tag_id_bf += temp(1)
        tag_name_bf += temp(2)
      }
    }
    tag_id_bf.exists(v => Configure.tags("banned").contains(v))
  }

  def get_hot_value() = {

  }
}
