package user_item

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Wind on 2018/07/04.
  * generate the user-item rating according user action
  */
object User_Item_Rating {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.ERROR)

    // val job_time: String = args(0)
    val job_time: String = "20180520"
    val day_offset: Int = 0
    val month_offset: Int = 0
    val (job_date, algo_date) = get_date(job_time, month_offset, day_offset)
    //val date = new SimpleDateFormat("yyyyMMdd").parse(yestoday_date)



  }

  def get_date(job_time: String, month_offset: Int, day_offset: Int): (String, String) = {
    val date = new SimpleDateFormat("yyyyMMdd").parse(job_time)
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(date)
    val job_date = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)
    printf("\n====>>>> job_date: %s\n", job_date)

    if (day_offset != 0)
      calendar.add(Calendar.DATE, day_offset)
    if (month_offset != 0)
      calendar.add(Calendar.MONTH, month_offset)

    val algo_date = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)
    printf("\n====>>>> algo_date: %s\n", algo_date)

    (job_date, algo_date)
  }

  def get_user_event_sdk(hiveContext:HiveContext,
                         user_event_table:String,
                         yestoday_date:String,
                         event_score_map:Map[String, Double]):RDD[(String,String,String,String,Long)]={
    val select_user_event_sql_android = "select platform['_imei_'] as imei, event_values['content_id'] as content_id, event_infos['_name_'] as event_name, event_values['rate'] as event_value, event_infos['_time_'] as time from " + user_event_table + " where stat_date like '" + yestoday_date + "%'"
    val user_event_df_android = hiveContext.sql(select_user_event_sql_android).filter("imei is not null and content_id is not null and event_name is not null and time is not null")
    val select_user_event_sql_ios = "select platform['_ios_uuid_'] as imei, event_values['content_id'] as content_id, event_infos['_name_'] as event_name, event_values['rate'] as event_value, event_infos['_time_'] as time from " + user_event_table +" where stat_date like '" + yestoday_date+ "%'"
    val user_event_df_ios = hiveContext.sql(select_user_event_sql_ios).filter("imei is not null and content_id is not null and event_name is not null and time is not null")
    val user_event_rdd = user_event_df_android.union(user_event_df_ios).rdd.map(v=>{
      var rate = "0"
      var flag = true
      if(v.getString(3)!=null&&v.getString(3)!="inf")
        rate = v.getString(3)
      try{
        if(v.getString(4).toDouble<1510243200000d)
          flag = false
      }
      catch{
        case _:NumberFormatException=>println("time exception")
          flag = false
      }
      (v.getString(0).trim.toLowerCase,v.getString(1).trim,v.getString(2).trim,rate,v.getString(4),flag)
    }).filter(v=>event_score_map.contains(v._3)&&v._4.toDouble>=0 &&v._1!=""&&v._2!=""&&v._6).map(v=>(v._1,v._2,v._3,v._4,v._5.toLong))
    user_event_rdd
  }

  /* def save_user_item_rating(hiveContext:HiveContext,
                             user_item_rating_table:String,
                             user_content_rating:RDD[(String, String,String, Double,Long)],
                             yestoday_date:String)={
     val create_user_item_table_sql = "create table if not exists "+user_item_rating_table+" (imei string,item_id string,event string,rating double) partitioned by (stat_date bigint) STORED AS RCFILE"
     import hiveContext.implicits._
     val user_item_df = user_content_rating.map(v=>User_Item_Rating(v._1,v._2,v._3,v._4)).toDF()
     val user_item_rgs_table_name:String = "user_item_rgs_table"
     user_item_df.registerTempTable(user_item_rgs_table_name)
     val insert_user_item_sql:String = "insert overwrite table "+user_item_rating_table+" partition(stat_date = "+yestoday_date+") select * from "+user_item_rgs_table_name
     hiveContext.sql(create_user_item_table_sql)
     hiveContext.sql(insert_user_item_sql)
   }
 */
//  def save_user_item_rating_time(hiveContext:HiveContext,
//                                 user_item_rating_table:String,
//                                 user_content_rating:RDD[(String, String,String, Double,Long)],
//                                 yestoday_date:String)={
//    val create_user_item_table_sql = "create table if not exists "+user_item_rating_table+" (imei string,item_id string,event string,rating double,time bigint) partitioned by (stat_date bigint) STORED AS RCFILE"
//    import hiveContext.implicits._
//    val user_item_df = user_content_rating.map(v=>User_Item_Rating_Time(v._1,v._2,v._3,v._4,v._5)).toDF()
//    val user_item_rgs_table_name:String = "user_item_rgs_table"
//    user_item_df.registerTempTable(user_item_rgs_table_name)
//    val insert_user_item_sql:String = "insert overwrite table "+user_item_rating_table+" partition(stat_date = "+yestoday_date+") select * from "+user_item_rgs_table_name
//    hiveContext.sql(create_user_item_table_sql)
//    hiveContext.sql(insert_user_item_sql)
//  }


}
