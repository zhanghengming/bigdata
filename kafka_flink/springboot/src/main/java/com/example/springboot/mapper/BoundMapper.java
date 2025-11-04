package com.example.springboot.mapper;

import com.example.springboot.beans.UserBean;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface BoundMapper {
    @Select("select * from user") // 因为返回的数据很多，我们用List接收，用实体bean来定义数据类型
    List<UserBean> selectAll();
}
