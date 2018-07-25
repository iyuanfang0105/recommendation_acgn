package items_repository

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Statistic_Report {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val job_time: String = args(0)
    // val job_time: String = "20180702"; val job_date = job_time; val algo_date = job_time
    val day_offset: Int = 0
    val month_offset: Int = 0
    val (job_date, algo_date) = utils.get_date(job_time, month_offset, day_offset)

    val algo_v_list: Array[String] = Array("v_ctr2", "v_lr_v1", "welfare_v1", "cartoon_v1", "animation_v1")

    val (app_pv, algo_pv) = get_pv(sparkSession, job_date)
    val (app_cv, algo_cv) = get_cv(sparkSession, job_date)
    val (app_duration, algo_duration) = get_duration(sparkSession, job_date)

    val result_array = algo_v_list.map(v => {
      val pv = algo_pv.getOrElse(v, (-1, -1))
      val cv = algo_cv.getOrElse(v, (-1, -1))
      val dr = algo_duration.getOrElse(v, -1)
      (v, pv._1.toString.toDouble, pv._2.toString.toDouble, cv._1.toString.toDouble, cv._2.toString.toDouble, dr.toString.toDouble)
    }) :+ ("app", app_pv(0)._1, app_pv(0)._2, app_cv(0)._1, app_cv(0)._2, app_duration(0))

    val result_rdd = sparkSession.sparkContext.parallelize(result_array).map(v => (v._1, v._2, v._3, v._4, v._5, v._6, v._4/v._2, v._5/v._3, v._6/v._3))
    val save_table_name = "algo.yf_acgn_data_report"
    save_to_db(sparkSession, result_rdd, save_table_name, job_date)
  }

  def get_pv(sparkSession: SparkSession, job_date: String) = {
    val select_sql: String = "select count(platform['_umid_']) as exposure_count,  count(DISTINCT platform['_umid_']) as exposure_device_count from mzacgn.ods_any_uxip_acgfun where stat_date like \"" + job_date + "%\" and event_infos ['_name_'] in (\"channel_item_exposure\", \"content_exposure\") and pkg_ver >=\"1.0.7\""
    printf("\n====>>>> %s\n", select_sql)
    val app_pv = sparkSession.sql(select_sql).rdd.map(v => (v(0).toString.toDouble, v(1).toString.toDouble)).collect()

    val select_sql_1: String = "SELECT event_values ['algoVersion'] as algo_v, count(platform['_umid_']) as exposure_count,  count(DISTINCT platform['_umid_']) as exposure_device_count from mzacgn.ods_any_uxip_acgfun where stat_date like \"" + job_date + "%\" and event_infos ['_name_'] in (\"channel_item_exposure\", \"content_exposure\") and pkg_ver >=\"1.0.7\" and event_values ['algoVersion'] in (\"v_ctr2\", \"v_lr_v1\", \"welfare_v1\", \"cartoon_v1\", \"animation_v1\") GROUP by event_values ['algoVersion']"
    printf("\n====>>>> %s\n", select_sql_1)
    val algo_pv = sparkSession.sql(select_sql_1).rdd.map(v => (v(0).toString, (v(1).toString.toDouble, v(2).toString.toDouble))).collectAsMap()

    (app_pv, algo_pv)
  }

  def get_cv(sparkSession: SparkSession, job_date: String) = {
    val select_sql: String = "select count(platform['_umid_']) as click_count,  count(DISTINCT platform['_umid_']) as click_device_count from mzacgn.ods_any_uxip_acgfun where stat_date like \"" + job_date + "%\" and event_infos ['_name_'] in (\"content_click\") and pkg_ver >=\"1.0.7\""
    printf("\n====>>>> %s\n", select_sql)
    val app_cv = sparkSession.sql(select_sql).rdd.map(v => (v(0).toString.toDouble, v(1).toString.toDouble)).collect()

    val select_sql_1: String = "SELECT event_values ['algoVersion'] as algo_v, count(platform['_umid_']) as exposure_count,  count(DISTINCT platform['_umid_']) as exposure_device_count from mzacgn.ods_any_uxip_acgfun where stat_date like \"" + job_date + "%\" and event_infos ['_name_'] in (\"content_click\") and pkg_ver >=\"1.0.7\" and event_values ['algoVersion'] in (\"v_ctr2\", \"v_lr_v1\", \"welfare_v1\", \"cartoon_v1\", \"animation_v1\") GROUP by event_values ['algoVersion']"
    printf("\n====>>>> %s\n", select_sql_1)
    val algo_cv = sparkSession.sql(select_sql_1).rdd.map(v => (v(0).toString, (v(1).toString.toDouble, v(2).toString.toDouble))).collectAsMap()
    (app_cv, algo_cv)
  }

  def get_duration(sparkSession: SparkSession, job_date: String) = {
    val select_sql: String = "select sum(cast(event_values['duration'] as bigint)) as duration from mzacgn.ods_any_uxip_acgfun where stat_date like \"" + job_date + "%\" and event_infos['_name_']=\"page_stay_time\" and event_values['page_name'] in (\"page_main_command\",\"Page_video_details\",\"Page_graphic_details\") and pkg_ver>=\"1.0.7\""
    printf("\n====>>>> %s\n", select_sql)
    val app_duration = sparkSession.sql(select_sql).rdd.map(v => v(0).toString.toDouble).collect()

    val select_sql_1: String = "select event_values ['algoVersion'] as algo_v, sum(cast(event_values['duration'] as bigint)) as duration from mzacgn.ods_any_uxip_acgfun where stat_date like \"" + job_date + "%\" and event_infos['_name_']=\"page_stay_time\" and event_values['page_name'] in (\"page_main_command\",\"Page_video_details\",\"Page_graphic_details\") and pkg_ver>=\"1.0.7\" and event_values ['algoVersion'] in (\"v_ctr2\", \"v_lr_v1\", \"welfare_v1\", \"cartoon_v1\", \"animation_v1\") GROUP by event_values ['algoVersion']"
    printf("\n====>>>> %s\n", select_sql_1)
    val algo_duration = sparkSession.sql(select_sql_1).rdd.map(v => (v(0).toString, v(1).toString.toDouble)).collectAsMap()
    (app_duration, algo_duration)
  }

  def save_to_db(sparkSession: SparkSession,
                 data: RDD[(String, Double, Double, Double, Double, Double, Double, Double, Double)],
                 table: String,
                 job_date: String) = {
    import sparkSession.implicits._
    val data_df = data.repartition(1).toDF("name", "pv", "pv_device", "uv", "uv_device", "duration", "pv_ctr", "uv_ctr", "duration_pp")
    data_df.createOrReplaceTempView("temp")

    val create_sql = "create table if not exists " + table + " (name string, pv bigint, pv_device bigint, uv bigint, uv_device bigint, duration bigint, pv_ctr double, uv_ctr double, duration_pp double) partitioned by (stat_date bigint) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE"
    val insert_sql: String = "insert overwrite table " + table + " partition(stat_date = " + job_date + ") select * from temp"
    sparkSession.sql(create_sql)
    sparkSession.sql(insert_sql)
  }
}
