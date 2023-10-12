package com.gus.batchreports.domain;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.OffsetDateTime;

@Entity
@Table(name = "users")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class User {

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "USERS_SEQ_GEN")
    @SequenceGenerator(name = "USERS_SEQ_GEN", sequenceName = "USERS_ID_SEQ", allocationSize = 1)
    private Long id;

    @Column(nullable = false)
    private String username;

    @Column(nullable = false)
    private String fullname;

    @Column(nullable = false)
    private OffsetDateTime createdAt;
}
