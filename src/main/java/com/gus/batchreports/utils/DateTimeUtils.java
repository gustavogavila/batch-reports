package com.gus.batchreports.utils;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class DateTimeUtils {
    public static String parseDateToString(String dateString) throws SQLException {
        DateTimeFormatter formatterForSeconds = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DateTimeFormatter formatterForNano = new DateTimeFormatterBuilder()
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 3, true)
                .toFormatter();

        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                .append(formatterForSeconds)
                .appendOptional(formatterForNano)
                .toFormatter()
                .withZone(ZoneId.systemDefault());

        ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateString, formatter);
        OffsetDateTime createdAt = zonedDateTime.toOffsetDateTime();

        DateTimeFormatter formatterForString = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return createdAt.format(formatterForString);
    }

    public static String parseDateToSimpleString(String dateString) {
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateString);
        OffsetDateTime createdAt = zonedDateTime.toOffsetDateTime();
        DateTimeFormatter formatterForString = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return createdAt.format(formatterForString);
    }
}
