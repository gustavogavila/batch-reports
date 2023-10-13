package com.gus.batchreports.reports;

import com.gus.batchreports.domain.UserReportDTO;
import lombok.extern.slf4j.Slf4j;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

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
                .sql("SELECT * FROM PUBLIC.USERS")
                .rowMapper((rs, rowNum) -> {
                    UserReportDTO user = new UserReportDTO();
                    user.setId(rs.getLong("id"));
                    user.setUsername(rs.getString("username"));
                    user.setFullname(rs.getString("fullname"));
                    String createdAtString = parseDateToString(rs);
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
    @Qualifier("USERS_REPORT_STEP")
    public Step usersReportStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                @Qualifier("USERS_REPORT_READER") JdbcCursorItemReader<UserReportDTO> usersReportReader,
                                @Qualifier("USERS_REPORT_WRITER") FlatFileItemWriter<UserReportDTO> usersReportWriter) {
        return new StepBuilder("USERS_REPORT_STEP", jobRepository)
                .<UserReportDTO, UserReportDTO>chunk(100, transactionManager)
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

    private static String parseDateToString(ResultSet rs) throws SQLException {
        DateTimeFormatter formatterForSeconds = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter formatterForNano = new DateTimeFormatterBuilder()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 3, true)
                .toFormatter();

        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .append(formatterForSeconds)
                .appendOptional(formatterForNano)
                .toFormatter()
                .withZone(ZoneId.systemDefault());

        ZonedDateTime zonedDateTime = ZonedDateTime.parse(rs.getString("created_at"), formatter);
        OffsetDateTime createdAt = zonedDateTime.toOffsetDateTime();

        DateTimeFormatter formatterForString = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String createdAtString = createdAt.format(formatterForString);
        return createdAtString;
    }
}
