package com.example.springboot.controller;

import com.example.springboot.service.BoundService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BoundController {
    @Autowired
    private BoundService boundService;

    @RequestMapping("/selectAll")
    public String selectAll() {
        return boundService.getAll().toString();
    }
}
