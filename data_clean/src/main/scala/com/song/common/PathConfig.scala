package com.song.common

import com.song.utils.DateUtils

object PathConfig {

  val rootPath = "/home/song/clean"

  val mobilePrefix = "/mobile"
  val mobileHistoryBillPath = rootPath + mobilePrefix + "/historyBill/" + DateUtils.getYesterday()
  val mobileBillInfoPath = rootPath + mobilePrefix + "/billInfo/" + DateUtils.getYesterday()
  val mobileUserInfoPath = rootPath + mobilePrefix + "/userInfo/" + DateUtils.getYesterday()
  val mobileCallDetailPath = rootPath + mobilePrefix + "/callDetail/" + DateUtils.getYesterday()
  val mobileNetDetailPath = rootPath + mobilePrefix + "/netDetail/" + DateUtils.getYesterday()
  val mobileSmsDetailPath = rootPath + mobilePrefix + "/smsDetail/" + DateUtils.getYesterday()
  val mobilePaymentRecordPath = rootPath + mobilePrefix + "/paymentRecord/" + DateUtils.getYesterday()
  val mobilePhoneInfoPath = rootPath + mobilePrefix + "/phoneInfo/" + DateUtils.getYesterday()

}
