package com.gus.batchreports.services;

import com.gus.batchreports.domain.User;
import com.gus.batchreports.repositories.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final JobLauncher customJobLauncher;
    private final Job job;

    public List<User> getAll() {
        return userRepository.findAll();
    }

    public void getReport() throws Exception {
        customJobLauncher.run(job, new JobParameters());
    }
}
