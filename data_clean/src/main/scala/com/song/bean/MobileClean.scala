package com.song.bean

import com.alibaba.fastjson.{JSON, JSONObject}
import com.song.common.PathConfig
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object MobileClean {

  val cleanFunc = (sc:SparkContext,inputPath:String)=>{
    val sourceRdd = sc.textFile(inputPath)
    val historyRdd = sourceRdd.flatMap(data=>{
      val jSONObject = JSON.parseObject(data);
      val phoneNo = jSONObject.getString("phone_no")
      val historyBillInfo = jSONObject.getJSONObject("historyBillInfo");
      val keySet = historyBillInfo.keySet();
      val listBuilder : ListBuffer[(String,String,JSONObject)] = ListBuffer()
      for(key <- keySet){
        val historyBill = historyBillInfo.getJSONObject(key);
        listBuilder.append((phoneNo,key,historyBill))
      }
      listBuilder
    })

    val historyFormatRdd = historyRdd.map(data=>{
      val phoneNo = data._1
      val month = data._2
      val historyBill = data._3
      val listBuffer : ListBuffer[String] = ListBuffer()
      val result = phoneNo + "\001" + month +
          "\001" + historyBill.getString("recvFeeUsed") +
          "\001" + historyBill.getString("writeOffFee") +
          "\001" + historyBill.getString("allFee")
      result
    })
    val billInfosRdd = historyRdd.flatMap(data=>{
      val phoneNo = data._1
      val month = data._2
      val billInfos = data._3.getJSONArray("billInfos")
      val listBuffer : ListBuffer[String] = ListBuffer()

      for(i <- 0 until billInfos.length){
        val billInfo = billInfos.getJSONObject(i)

        val result = phoneNo+"\001"+month + "\001" + billInfo.getString("busiName") +"\001"+ billInfo.getString("fee")
        listBuffer.append(result)
      }
      listBuffer
    })


    val userInfoRdd = sourceRdd.map(data=>{
      val jSONObject = JSON.parseObject(data);
      val phoneNo = jSONObject.getString("phone_no")
      val userInfo = jSONObject.getJSONObject("userInfo");
      val result = phoneNo +
                    "\001" + userInfo.getString("certType") +
                    "\001" + userInfo.getString("certAddr") +
                    "\001" + userInfo.getString("sex") +
                    "\001" + userInfo.getString("certNum") +
                    "\001" + userInfo.getString("userName") +
                    "\001" + userInfo.getString("email")
      result
    })


    val paymentRecordRdd = sourceRdd.flatMap(data=>{
      val jSONObject = JSON.parseObject(data);
      val phoneNo = jSONObject.getString("phone_no")
      val paymentRecords = jSONObject.getJSONArray("paymentRecord");
      val listBuilder : ListBuffer[String] = ListBuffer()
      for(i <- 0 until paymentRecords.length){
        val paymentRecord = paymentRecords.getJSONObject(i)
        val result = phoneNo +
                    "\001" + paymentRecord.getString("payfee") +
                    "\001" + paymentRecord.getString("paychannel") +
                    "\001" + paymentRecord.getString("paymentid") +
                    "\001" + paymentRecord.getString("payment") +
                    "\001" + paymentRecord.getString("paydate")

        listBuilder.append(result)
      }
      listBuilder
    })

    val callDetailRdd = sourceRdd.flatMap(data=>{
      val jSONObject = JSON.parseObject(data);
      val phoneNo = jSONObject.getString("phone_no")
      val callDetails = jSONObject.getJSONArray("callDetail");
      val listBuilder : ListBuffer[String] = ListBuffer()
      for(i <- 0 until callDetails.length){
        val callDetail = callDetails.getJSONObject(i)
        val result = phoneNo +
          "\001" + callDetail.getString("othernum") +
          "\001" + callDetail.getString("twoplusfee") +
          "\001" + callDetail.getString("otherfee") +
          "\001" + callDetail.getString("roamfee") +
          "\001" + callDetail.getString("cellid") +
          "\001" + callDetail.getString("thtype") +
          "\001" + callDetail.getString("landtype") +
          "\001" + callDetail.getString("businesstype") +
          "\001" + callDetail.getString("homeareaName") +
          "\001" + callDetail.getString("romatypeName") +
          "\001" + callDetail.getString("totalfee") +
          "\001" + callDetail.getString("otherarea") +
          "\001" + callDetail.getString("thtypeName") +
          "\001" + callDetail.getString("calledhome") +
          "\001" + callDetail.getString("calltype") +
          "\001" + callDetail.getString("landfee") +
          "\001" + callDetail.getString("longtype") +
          "\001" + callDetail.getString("calltypeName") +
          "\001" + callDetail.getString("homenum") +
          "\001" + callDetail.getString("calllonghour") +
          "\001" + callDetail.getString("homearea") +
          "\001" + callDetail.getString("nativefee") +
          "\001" + callDetail.getString("romatype") +
          "\001" + callDetail.getString("calltime") +
          "\001" + callDetail.getString("calldate") +
          "\001" + callDetail.getString("deratefee")


        listBuilder.append(result)
      }
      listBuilder
    })

    val netDetailRdd = sourceRdd.flatMap(data=>{
      val jSONObject = JSON.parseObject(data);
      val phoneNo = jSONObject.getString("phone_no")
      val netDetails = jSONObject.getJSONArray("netDetail");
      val listBuilder : ListBuffer[String] = ListBuffer()
      for(i <- 0 until netDetails.length){
        val netDetail = netDetails.getJSONObject(i)
        val result = phoneNo +
          "\001" + netDetail.getString("accessip") +
          "\001" + netDetail.getString("bizname") +
          "\001" + netDetail.getString("featinfo") +
          "\001" + netDetail.getString("domainname") +
          "\001" + netDetail.getString("totaltraffic") +
          "\001" + netDetail.getString("useragent") +
          "\001" + netDetail.getString("begintime") +
          "\001" + netDetail.getString("flowtype") +
          "\001" + netDetail.getString("uptraffic") +
          "\001" + netDetail.getString("biztype") +
          "\001" + netDetail.getString("durationtime") +
          "\001" + netDetail.getString("clientip") +
          "\001" + netDetail.getString("flowname") +
          "\001" + netDetail.getString("rattype") +
          "\001" + netDetail.getString("downtraffic") +
          "\001" + netDetail.getString("apn")

        listBuilder.append(result)
      }
      listBuilder
    })


    val smsDetailRdd = sourceRdd.flatMap(data=>{
      val jSONObject = JSON.parseObject(data);
      val phoneNo = jSONObject.getString("phone_no")
      val smsDetails = jSONObject.getJSONArray("smsDetail");
      val listBuilder : ListBuffer[String] = ListBuffer()
      for(i <- 0 until smsDetails.length){
        val smsDetail = smsDetails.getJSONObject(i)
        val result = phoneNo +
          "\001" + smsDetail.getString("smstime") +
          "\001" + smsDetail.getString("amount") +
          "\001" + smsDetail.getString("othernum") +
          "\001" + smsDetail.getString("smstype") +
          "\001" + smsDetail.getString("deratefee") +
          "\001" + smsDetail.getString("fee") +
          "\001" + smsDetail.getString("homearea") +
          "\001" + smsDetail.getString("otherarea") +
          "\001" + smsDetail.getString("smsdate") +
          "\001" + smsDetail.getString("businesstype")

        listBuilder.append(result)
      }
      listBuilder
    })


    val phoneInfoRdd = sourceRdd.map(data=>{
      val jSONObject = JSON.parseObject(data);
      val operator = jSONObject.getString("operator")
      val createTime = jSONObject.getString("createTime")
      val phoneInfo = jSONObject.getJSONObject("phoneInfo");
      val result =
          phoneInfo.getString("realFee") +
          "\001" + phoneInfo.getString("userStatus") +
          "\001" + phoneInfo.getString("roamstat") +
          "\001" + phoneInfo.getString("totalScore") +
          "\001" + phoneInfo.getString("inNetDate") +
          "\001" + phoneInfo.getString("phoneNumber") +
          "\001" + operator +
          "\001" + phoneInfo.getString("userLevel") +
          "\001" + phoneInfo.getString("balance") +
          "\001" + phoneInfo.getString("totalCreditValue") +
          "\001" + phoneInfo.getString("landLevel") +
          "\001" + phoneInfo.getString("packageName") +
          "\001" + phoneInfo.getString("brand")+
          "\001" + createTime

      result
    })

    historyFormatRdd.saveAsTextFile(PathConfig.mobileHistoryBillPath)
    billInfosRdd.saveAsTextFile(PathConfig.mobileBillInfoPath)
    userInfoRdd.saveAsTextFile(PathConfig.mobileUserInfoPath)
    paymentRecordRdd.saveAsTextFile(PathConfig.mobilePaymentRecordPath)
    callDetailRdd.saveAsTextFile(PathConfig.mobileCallDetailPath)
    netDetailRdd.saveAsTextFile(PathConfig.mobileNetDetailPath)
    smsDetailRdd.saveAsTextFile(PathConfig.mobileSmsDetailPath)
    phoneInfoRdd.saveAsTextFile(PathConfig.mobilePhoneInfoPath)
  }
}
