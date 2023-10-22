package com.gus.batchreports.reports;

import com.gus.batchreports.domain.UserReportDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.DisposableBean;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

@Slf4j
public class CustomItemWriter implements ItemWriter<UserReportDTO>, DisposableBean {

    private final String requestDate;
    private final String requestUser;
    private final CSVPrinter printer;

    public CustomItemWriter(String requestDate, String requestUser) throws IOException {
        this.requestDate = requestDate;
        this.requestUser = requestUser;

        String fileName = String.format("reports_out/USERS_REPORT__%s__%s.csv", requestDate, requestUser);

        String[] HEADERS = {"id","username","fullname","createdAt"};
        CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader(HEADERS).build();
        Writer writer = new FileWriter(fileName, true);
        printer = new CSVPrinter(writer, csvFormat);
    }

    @Override
    public void write(Chunk<? extends UserReportDTO> chunk) throws Exception {
        List<? extends UserReportDTO> items = chunk.getItems();
            for (UserReportDTO user : items) {
                printer.printRecord(user.getId(),
                        user.getUsername(),
                        user.getFullname(),
                        user.getCreatedAt().toString());
                printer.flush();
            }
    }

    @Override
    public void destroy() throws Exception {
        if (printer != null) {
            printer.flush();
            printer.close();
        }
    }
}
