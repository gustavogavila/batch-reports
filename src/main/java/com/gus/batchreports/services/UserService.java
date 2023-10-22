package com.gus.batchreports.services;

import com.gus.batchreports.domain.User;
import com.gus.batchreports.repositories.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final JobLauncher customJobLauncher;
    private final Job job;

    public List<User> getAll() {
        return userRepository.findAll();
    }

    public void getReport(String requestUser) throws Exception {
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("jobUUID", UUID.randomUUID().toString())
                .addString("dataExecucao", OffsetDateTime.now().toString())
                .addString("requestUser", requestUser)
                .toJobParameters();
        customJobLauncher.run(job, jobParameters);
    }
}
