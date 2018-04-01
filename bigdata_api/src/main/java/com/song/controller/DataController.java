package com.song.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SuccessCallback;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@RestController
public class DataController {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @RequestMapping("index")
    public String index(HttpServletRequest request){

        int len = request.getContentLength();

        if(len <= 0){
            return "error";
        }

        String json = null;
        try {
            ServletInputStream  iii = request.getInputStream();
            byte[] buffer = new byte[len];
            iii.read(buffer, 0, len);
            json = new String(buffer,"UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        }

        ListenableFuture send = kafkaTemplate.send("mobileinfo", "key",json);
        send.addCallback(new SuccessCallback() {
            @Override
            public void onSuccess(Object result) {
                System.out.println("发送成功");
            }
        }, new FailureCallback() {
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("发送失败");
            }
        });
        return "hello";
    }
}
