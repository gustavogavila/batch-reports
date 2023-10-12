package com.gus.batchreports.reports;

import com.gus.batchreports.domain.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class UsersReportJobConfig {


    @Bean
    @Qualifier("USERS_REPORT_READER")
    public JdbcCursorItemReader<User> usersReportReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<User>()
                .name("USERS_REPORT_READER")
                .dataSource(dataSource)
                .sql("SELECT * FROM PUBLIC.USERS")
                .rowMapper((rs, rowNum) -> {
                    User user = new User();
                    user.setId(rs.getLong("id"));
                    user.setUsername(rs.getString("username"));
                    user.setFullname(rs.getString("fullname"));
//                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
//                            .withZone(ZoneId.systemDefault());
//                    OffsetDateTime createdAt = OffsetDateTime.parse(rs.getString("created_at"), dateTimeFormatter);
//                    user.setCreatedAt(createdAt);
                    return user;
                })
                .build();
    }

    @Bean
    @Qualifier("USERS_REPORT_WRITER")
    public FlatFileItemWriter<User> usersReportWriter() {
        return new FlatFileItemWriterBuilder<User>()
                .name("USERS_REPORT_WRITER")
                .resource(new FileSystemResource("reports_out/file"))
                .delimited()
                .delimiter(",")
                .names("id", "username", "fullname", "createdAt")
                .headerCallback(writer -> writer.write("id,username,fullname,createdAt"))
                .build();
    }

    @Bean
    @Qualifier("USERS_REPORT_STEP")
    public Step usersReportStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                @Qualifier("USERS_REPORT_READER") JdbcCursorItemReader<User> usersReportReader,
                                @Qualifier("USERS_REPORT_WRITER") FlatFileItemWriter<User> usersReportWriter) {
        return new StepBuilder("USERS_REPORT_STEP", jobRepository)
                .<User, User>chunk(100, transactionManager)
                .reader(usersReportReader)
                .writer(usersReportWriter)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    @Qualifier("USERS_REPORT_JOB")
    public Job usersReportJob(JobRepository jobRepository,
                              @Qualifier("USERS_REPORT_STEP") Step usersReportStep) {
        return new JobBuilder("USERS_REPORT_JOB", jobRepository)
                .start(usersReportStep)
                .incrementer(new RunIdIncrementer())
                .build();
    }
}
