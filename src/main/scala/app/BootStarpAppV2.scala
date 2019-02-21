package app

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import utils._

object BootStarpAppV2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("中国移动运营实时监控平台-Monitor")
    //如果在集群上运行的话，需要去掉：sparkConf.setMaster（“local[*]”）
    sparkConf.setMaster("local[*]")
    //将rdd以序列化格式来保存以减少内存的占用
    //默认采用org.apache.spark.serializer.JavaSerializer
    //这是最基本的优化
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //rdd压缩
    sparkConf.set("spark.rdd.compress", "true")
    //batchSize = partitionNum * 100(分区数量) * 采样时间
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "100")
    //优雅的停止
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    /**
      * 提取数据库中存储的偏移量
      */
    val currOffset = OffsetManager.getMydbCurrentOffset

    /**
      * 广播省份映射关系  broadcast 广播
      */
      val pcode2pName = ssc.sparkContext.broadcast(AppParams.pCode2pName)

    //获取kafka的数据
    val stream = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](AppParams.topic,
        AppParams.kafkaParams,
        currOffset
      )
    )

    /**
      * 数据处理
      */
    stream.foreachRDD(rdd => {
      //取得所有充值通知日志
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val baseData = KpiTools.baseDataRDD(rdd)

      /**
        * 业务概述（总订单量，充值订单量，总金额，业务时间）
        */
      KpiTools.kpi_general(baseData)

      /**
        * 业务概述-每小时充值情况
        */
      KpiTools.kpi_general_hour(baseData)

      /**
        * 业务质量
        */
      KpiTools.kpi_quality(baseData, pcode2pName)

      /**
        * 实时充值情况分析
        */
      KpiTools.kpi_realtime_minute(baseData)

      /**
        * 实时存储偏移量
        */
      OffsetManager.saveCurrentOffset(offsetRanges)

    })

    ssc.start()
    ssc.awaitTermination()
  }


}
