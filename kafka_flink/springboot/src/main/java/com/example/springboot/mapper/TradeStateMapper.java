package com.example.springboot.mapper;

/* controller调用service层，service调用mapper层，mapper层调用数据库
 设计的原则是面向抽象编程，面向接口编程，而不是面向具体现编程，所以mapper层不需要实现类，只需要接口即可。
 接口的实现交给mybatis框架来完成*/
// 交易域统计mapper
// 如果按之前的操作，需要先定义一个类来实现该接口，然后在实现类中重写方法。
// 如果我们自己写的话，通过jdbc来实现，有标准的步骤。1. 注册驱动 2.获取连接 3.创建数据库操作对象 4.执行sql语句 5.处理结果集 6.释放资源
// 但是mybatis已经将这些步骤封装好了，自动创建接口实现类，并实现抽象方法。唯一不知道的就是sql语句怎么写。
// 我们就需要告诉框架该怎么写sql语句，就需要在接口上添加注解。增删改查

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;

public interface TradeStateMapper {
    // 获取某天下单总金额
    @Select("select sum(amount) from trade_state where date = #{date}")
//    @Insert()
//    @Update()
//    @Delete()
    BigDecimal selectTradeStateCount(String date);
}
