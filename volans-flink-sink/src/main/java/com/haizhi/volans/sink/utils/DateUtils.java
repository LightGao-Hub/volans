package com.haizhi.volans.sink.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Create by zhoumingbing on 2020-09-24
 */
public class DateUtils {
    private static final String ISO_8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    private static final String LOCAL_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final String LOCAL_DATE_FORMAT = "yyyy-MM-dd";
    public static final Long HALF_HOUR_MS = 1800000L;
    public static final Long ONE_YEAR_SECONDS = 31536000L;
    public static final Long ONE_YEAR_MS = 525600000L;

    public DateUtils() {
    }

    public static String getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date());
    }

    public static String getYesterday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(getDaysBefore(new Date(), 1));
    }

    public static Date getYearsBefore(Date d, int years) {
        Calendar now = Calendar.getInstance();
        now.setTime(d);
        now.set(1, now.get(1) - years);
        return now.getTime();
    }

    public static Date getDaysBefore(Date d, int days) {
        Calendar now = Calendar.getInstance();
        now.setTime(d);
        now.set(5, now.get(5) - days);
        return now.getTime();
    }

    public static Date getHoursBefore(Date d, int hours) {
        Calendar now = Calendar.getInstance();
        now.setTime(d);
        now.set(10, now.get(10) - hours);
        return now.getTime();
    }

    public static String getTodayAndHour() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
        return sdf.format(new Date());
    }

    public static String getTodayAndHourBefore() {
        Date date = getHoursBefore(new Date(), 1);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
        return sdf.format(date);
    }

    public static String formatLocal(long millis) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(millis);
    }

    public static String formatLocal(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(date);
    }

    public static String formatLocalDate(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(date);
    }

    public static String formatDate(Date date,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    public static String formatCurrentDate(String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date());
    }

    public static String toUTC(String time) {
        return toUTC(toLocal(time));
    }

    public static String toUTC(Date date) {
        return DateFormatUtils.format(date, "yyyy-MM-dd'T'HH:mm:ss'Z'");
    }

    public static String utc2Local(String utcTime) {
        return utc2Local(utcTime, "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd HH:mm:ss");
    }

    public static String utc2Local(String utcTime, String utcPatten, String localPatten) {
        Date utcDate = utc2Local(utcTime, utcPatten);
        if (utcDate == null) {
            return utcTime;
        } else {
            SimpleDateFormat local = new SimpleDateFormat(localPatten);
            local.setTimeZone(TimeZone.getDefault());
            String localTime = local.format(utcDate.getTime());
            return localTime;
        }
    }

    public static Date utc2Local(String utcTime, String utcPatten) {
        SimpleDateFormat utc = new SimpleDateFormat(utcPatten);
        utc.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date utcDate = null;

        try {
            utcDate = utc.parse(utcTime);
        } catch (ParseException var5) {
            var5.printStackTrace();
        }

        return utcDate;
    }

    public static long toLocalMillis(String time) {
        return toLocal(time).getTime() / 1000L;
    }

    public static Date toLocal(String time) {
        if (StringUtils.isBlank(time)) {
            return new Date();
        } else if (time.contains("T")) {
            return utc2Local(time, "yyyy-MM-dd'T'HH:mm:ss'Z'");
        } else {
            String format = "";
            time = time.replaceAll("[-|:| ]", "");
            if (time.length() == 8) {
                format = "yyyyMMdd";
            } else if (time.length() == 14) {
                format = "yyyyMMddHHmmss";
            }

            SimpleDateFormat sdf = new SimpleDateFormat(format);

            Date date;
            try {
                date = sdf.parse(time);
            } catch (ParseException var5) {
                var5.printStackTrace();
                date = new Date();
            }

            return date;
        }
    }

    public static Date parseDate(String time) throws ParseException {
        if (StringUtils.isBlank(time)) {
            return null;
        } else {
            if (time.contains("T")) {
                Date date = utc2Local(time, "yyyy-MM-dd'T'HH:mm:ss'Z'");
                if (date == null) {
                    return null;
                }
            }

            String format = "";
            time = time.replaceAll("[-|:| ]", "");
            if (time.length() == 8) {
                format = "yyyyMMdd";
            } else if (time.length() == 14) {
                format = "yyyyMMddHHmmss";
            }

            Date date = null;
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            date = sdf.parse(time);
            return date;
        }
    }

    public static boolean isDate(String time) throws ParseException {
        Date date = parseDate(time);
        return !Objects.isNull(date);
    }

    public static Date tryConvertDate(String value) {
        Date date = null;
        if (StringUtils.isBlank(value)) {
            return null;
        } else {
            try {
                SimpleDateFormat simpleDateFormat = null;
                if (value.length() == "yyyy-MM-dd HH:mm:ss".length()) {
                    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                } else if (value.length() == "yyyy-MM-dd".length()) {
                    simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                }

                if (null == simpleDateFormat) {
                    return null;
                }

                date = simpleDateFormat.parse(value);
            } catch (ParseException var3) {
            }

            return date;
        }
    }

    public static Date atEndOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(11, 23);
        calendar.set(12, 59);
        calendar.set(13, 59);
        calendar.set(14, 999);
        return calendar.getTime();
    }

    public static Date atStartOfDay(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(11, 0);
        calendar.set(12, 0);
        calendar.set(13, 0);
        calendar.set(14, 0);
        return calendar.getTime();
    }

    public static void main(String[] args) throws ParseException {
        System.out.println(utc2Local("2018-05-01T10:00:03.000Z", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd HH:mm:ss"));
        System.out.println(utc2Local("2018-05-01T10:00:03Z"));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        System.out.println(sdf.format(new Date()));
        System.out.println(toUTC(getDaysBefore(new Date(), 3)));
        System.out.println(toUTC(getYearsBefore(new Date(), 3)));
        System.out.println(StringUtils.substringBefore(toUTC(new Date()), ":"));
        System.out.println(getYesterday());
    }
}
