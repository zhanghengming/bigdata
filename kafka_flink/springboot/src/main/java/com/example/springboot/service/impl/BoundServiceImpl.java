package com.example.springboot.service.impl;

import com.example.springboot.beans.UserBean;
import com.example.springboot.mapper.BoundMapper;
import com.example.springboot.service.BoundService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BoundServiceImpl implements BoundService {
    @Autowired
    private BoundMapper boundMapper;
    @Override
    public List<UserBean> getAll() {
        return boundMapper.selectAll();
    }
}
