package com.gus.batchreports.controllers;

import com.gus.batchreports.domain.User;
import com.gus.batchreports.services.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @GetMapping
    public ResponseEntity<List<User>> getAll() {
        return ResponseEntity.ok(userService.getAll());
    }

    @GetMapping("report")
    public ResponseEntity<String> getReport() throws Exception {
        userService.getReport();
        return ResponseEntity.ok("Job iniciado com sucesso!");
    }
}
