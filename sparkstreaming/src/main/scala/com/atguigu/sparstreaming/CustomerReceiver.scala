package com.atguigu.sparstreaming

import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver



class CustomerReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
  //读取数据，写给spark
  override def onStart(): Unit = {
    //用另一个线程去做
    new Thread("receiver"){
      override def run(): Unit = {
        receiver()
      }
    }.start()
  }
  //实际读取数据
  def receiver():Unit={
    //创建Socket
    new Socket()
  }

  override def onStop(): Unit = ???
}
