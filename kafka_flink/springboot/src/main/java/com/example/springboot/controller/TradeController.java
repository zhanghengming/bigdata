package com.example.springboot.controller;


import com.example.springboot.service.TradeService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Date;

// 接收客户端发送的请求，做处理
@RestController
public class TradeController {

    @Autowired
    private TradeService tradeService;
    // 定义一个方法来处理客户端发送的
    // 拦截请求
    @RequestMapping("/getTradeCount")
    public String getTradeCount(@RequestParam(value = "date", defaultValue = "2020-01-01") String date) {
        if (date.equals("2020-01-01")) {
            date = DateUtils.addDays(new Date(), -1).toString();
        }
        BigDecimal count = tradeService.getTradeStateCount(date);
        return count.toString();
    }
}
