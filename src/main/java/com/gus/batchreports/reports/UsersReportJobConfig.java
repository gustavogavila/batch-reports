package com.gus.batchreports.reports;

import com.gus.batchreports.domain.UserReportDTO;
import com.gus.batchreports.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.IOException;

@Slf4j
@Configuration
public class UsersReportJobConfig {


    @Bean
    @Qualifier("USERS_REPORT_READER")
    public JdbcCursorItemReader<UserReportDTO> usersReportReader(DataSource dataSource) {
        log.info("INICIANDO LEITURA DOS USUARIOS PARA RELATORIO");
        return new JdbcCursorItemReaderBuilder<UserReportDTO>()
                .name("USERS_REPORT_READER")
                .dataSource(dataSource)
                .sql("""
                        SELECT * FROM PUBLIC.USERS
                     """)
                .rowMapper((rs, rowNum) -> {
                    UserReportDTO user = new UserReportDTO();
                    user.setId(rs.getLong("id"));
                    user.setUsername(rs.getString("username"));
                    user.setFullname(rs.getString("fullname"));
                    String createdAtString = DateTimeUtils.parseDateToString(rs.getString("created_at"));
                    user.setCreatedAt(createdAtString);
                    return user;
                })
                .build();
    }

    @Bean
    @Qualifier("USERS_REPORT_WRITER")
    public FlatFileItemWriter<UserReportDTO> usersReportWriter() {

        return new FlatFileItemWriterBuilder<UserReportDTO>()
                .name("USERS_REPORT_WRITER")
                .resource(new FileSystemResource("reports_out/file"))
                .delimited()
                .delimiter(",")
                .names("id", "username", "fullname", "createdAt")
                .headerCallback(writer -> writer.write("id,username,fullname,createdAt"))
                .build();
    }

    @Bean
    @JobScope
    @Qualifier("CUSTOM_ITEM_WRITER_USER")
    public CustomItemWriter customItemWriterUser(@Value("#{jobParameters['dataExecucao']}") String requestDate,
                                                 @Value("#{jobParameters['requestUser']}") String requestUser) throws IOException {
        String formattedDateTime = DateTimeUtils.parseDateToSimpleString(requestDate);
        return new CustomItemWriter(formattedDateTime, requestUser);
    }

    @Bean
    @Qualifier("USERS_REPORT_STEP")
    public Step usersReportStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                @Qualifier("USERS_REPORT_READER") JdbcCursorItemReader<UserReportDTO> usersReportReader,
                                @Qualifier("CUSTOM_ITEM_WRITER_USER") CustomItemWriter customItemWriter
    ) {
        return new StepBuilder("USERS_REPORT_STEP", jobRepository)
                .<UserReportDTO, UserReportDTO>chunk(100, transactionManager)
                .reader(usersReportReader)
                .writer(customItemWriter)
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
