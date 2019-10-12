package job

object Factory {

  def factory(): Unit ={
    val job=new Job()
    job.taskone()
  }

}
