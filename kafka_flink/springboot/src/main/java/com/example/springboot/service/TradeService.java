package com.example.springboot.service;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;


public interface TradeService {
    BigDecimal getTradeStateCount(String date);
}

