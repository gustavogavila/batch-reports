package com.gus.batchreports.domain;

import lombok.Data;

@Data
public class UserReportDTO {
    private Long id;
    private String username;
    private String fullname;
    private String createdAt;
}
