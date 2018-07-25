package items_repository

import utils.My_Utils

object Basement_Rating {
  def main(args: Array[String]): Unit = {
    val job_date = args(0)

    My_Utils.my_log("Initial spark")
    val sparkSession = My_Utils.init_job()


  }
}
