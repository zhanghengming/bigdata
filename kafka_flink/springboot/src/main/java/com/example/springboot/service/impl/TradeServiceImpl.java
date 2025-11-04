package com.example.springboot.service.impl;

import com.example.springboot.mapper.TradeStateMapper;
import com.example.springboot.service.TradeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

// 在干活的时候都需要创建对象，mapper里的对象是通过动态代理的方式创建的，是通过Spring容器创建的，所以这里不需要手动new一个对象
// 该实现类也需要创建对象，这里通过@Service注解，让Spring容器创建对象
/*
* - `@Service` 注解用于将 [TradeServiceImpl]类标记为 Spring 容器中的一个服务层组件，使其能够被自动扫描并注册到 Spring 的应用上下文中。
- `@Autowired` 注解则是用来自动装配（注入）其他已经在 Spring 容器中管理的 Bean。
- 因此，在类级别上使用 `@Service` 是为了声明该类是一个服务 Bean，而 `@Autowired` 用在字段或方法级别是为了实现依赖注入。两者作用不同，不能互相替代。
* */
@Service
public class TradeServiceImpl implements TradeService {

    @Autowired
    private TradeStateMapper tradeStateMapper;

    @Override
    public BigDecimal getTradeStateCount(String date) {
        return tradeStateMapper.selectTradeStateCount(date);
    }
}
