package utils

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat

object CaculateTools {

  //非线程安全的
  //private val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  //线程安全的DataFormat
  private val format = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")

  /**
    * 计算时间差
    * @param startTime
    * @param endTime
    * @return
    */
  def caculateTime(startTime:String, endTime:String):Long = {
    val start = startTime.substring(0, 17)
    format.parse(endTime).getTime - format.parse(start).getTime
  }
}
