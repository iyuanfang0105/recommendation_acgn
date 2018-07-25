package items_repository

import utils.My_Utils
import config.Configure

import java.text.SimpleDateFormat

object Basement_Items {
  def main(args: Array[String]): Unit = {
    val job_date = args(0)
    val (calender_now, time_now) = My_Utils.get_time_now()
    val time_before = My_Utils.time_offset(calender_now, month_offset = -3, day_offset = 0)

    My_Utils.my_log("Inital spark")
    val sparkSession = My_Utils.init_job()

    My_Utils.my_log("Get items infos")
    val items_info = Dataset.get_basement_items_info(sparkSession, Configure.tables("item_info_from_near_line_stream"), time_before.substring(0, 10), time_now)

    My_Utils.my_log("Save result to hive")
    import sparkSession.implicits._
    val columns_info = "content_id string, type string, author_id string, author_name string, channel string, publish_time string, update_time string, tag string, status string"
    My_Utils.save_result_to_hive(sparkSession, items_info.toDF(), columns_info, Configure.tables("basement_item_info"), job_date)
  }
}
