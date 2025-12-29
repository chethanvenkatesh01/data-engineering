package com.impact.utils.date;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@Slf4j
public class DateHelper {
    public static List<LocalDate> generateDateRange(String startDate, String endDate, boolean includeStartDate, boolean includeEndDate) {
        // startDate and endDate should be in the format of YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS+Z
        // if endDate is null, then current date is considered
        assert startDate!=null : "startDate cannot be null";
        Pattern pattern = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
        Matcher matcher = pattern.matcher(startDate);
        LocalDate startDt = null;
        LocalDate endDt = LocalDate.now();
        if(matcher.find()) {
            startDt = LocalDate.parse(matcher.group(), DateTimeFormatter.ISO_DATE);
        }
        else {
            throw new IllegalArgumentException(String.format("startDate %s cannot be matched with ISO date format YYYY-MM-DD", startDate));
        }
        if(endDate!=null) {
            matcher = pattern.matcher(endDate);
            if(matcher.find()) {
                endDt = LocalDate.parse(matcher.group(), DateTimeFormatter.ISO_DATE);
            }
            else {
                throw new IllegalArgumentException(String.format("endDate %s cannot be matched with ISO date format YYYY-MM-DD", endDate));
            }
        }
        startDt = includeStartDate ? startDt : startDt.plusDays(1);
        endDt = includeEndDate ? endDt.plusDays(1) : endDt;
        log.info(String.format("Start Date: %s, End Date: %s", startDt, endDt));
        List<LocalDate> dateRange = startDt.datesUntil(endDt).collect(Collectors.toList());
        return dateRange;
    }

    public static String getFileDateFromPath(String filePath) {
        log.debug(String.format("Fetching file date from file %s", filePath));
        List<String> patterns = new LinkedList<>();
        patterns.add("\\d{14}");
        patterns.add("\\d{12}");
        patterns.add("\\d{8}");
        patterns.add("\\d{4}-\\d{2}-\\d{2}");

        String datetimeStr = null;
        int patternIndex = -1;
        Pattern p = null;
        Matcher matcher = null;
        String parsedLocalDateTime = null;
        int i = 0;

        while (parsedLocalDateTime==null && i < patterns.size() ) {
            p = Pattern.compile(patterns.get(i));
            matcher = p.matcher(filePath);
            if(matcher.find()) {
                datetimeStr = matcher.group(0);
                patternIndex = i;
                try {
                    switch (patternIndex) {
                        case 0:
                            parsedLocalDateTime = LocalDateTime.parse(datetimeStr,
                                    DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toString() + (datetimeStr.endsWith("00") ? ":00" : "");
                            break;
                        case 1:
                            parsedLocalDateTime = LocalDateTime.parse(datetimeStr, DateTimeFormatter.ofPattern("yyyyMMddHHmm")).toString() + ":00";
                            break;
                        case 2:
                            parsedLocalDateTime = LocalDate.parse(datetimeStr, DateTimeFormatter.ofPattern("yyyyMMdd")).atStartOfDay().toString() + ":00";
                            break;
                        case 3:
                            parsedLocalDateTime = LocalDate.parse(datetimeStr, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay().toString() + ":00";
                            break;
                    }
                }
                catch (Exception e) {
                    log.error(ExceptionUtils.getStackTrace(e));
                }
            }
            i += 1;
        }

        if(parsedLocalDateTime==null) {
            parsedLocalDateTime = LocalDateTime.now().toString();
            log.debug(String.format("Couldn't extract datetime from the file %s. Using current datetime %s",
                    filePath, parsedLocalDateTime));
        }

        return parsedLocalDateTime;

    }
}
