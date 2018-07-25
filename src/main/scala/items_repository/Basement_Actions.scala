package items_repository

import utils.My_Utils
import config.Configure

object Basement_Actions {
  def main(args: Array[String]): Unit = {
    val job_date = args(0)

    My_Utils.my_log("Initial spark")
    val sparkSession = My_Utils.init_job()

    My_Utils.my_log("Geting basemnet actions")
    val actions = Dataset.get_basement_actions(sparkSession, Configure.tables("actions"), job_date, debug = true)
    My_Utils.print_count(actions, "actions")

    My_Utils.my_log("Save to hive")
    val colums = "(imei string, algo_v string, content_id string, actions string, actions_value string)"
    My_Utils.save_result_to_hive(sparkSession, actions, colums, Configure.tables("basement_actions_info"), job_date)
  }
}
