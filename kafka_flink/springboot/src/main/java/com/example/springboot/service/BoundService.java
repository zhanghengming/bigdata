package com.example.springboot.service;

import com.example.springboot.beans.UserBean;

import java.util.List;

public interface BoundService {
    List<UserBean> getAll();
}
