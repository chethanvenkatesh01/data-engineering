package com.impact.utils.text;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

public class StringUtils {

    public static String truncateQuotesEnclosingFields(String s, String fieldDelimiter) {
        //This method is used to remove the quotes which encloses the field values
        //while preserving the internal quotes
        // Ex: "A"|"B"|"C" -> A|B|C
        List<Pattern> patterns = new LinkedList<>();
        Pattern p1 = null;
        if(fieldDelimiter.equals("|")) {
            patterns.add(Pattern.compile("['\"]\\|['\"]"));
            patterns.add(Pattern.compile("['\"]\\|"));
            patterns.add(Pattern.compile("\\|['\"]"));
        }
        else {
            patterns.add(Pattern.compile(String.format("['\"]%s['\"]", fieldDelimiter)));
            patterns.add(Pattern.compile(String.format("['\"]%s", fieldDelimiter)));
            patterns.add(Pattern.compile(String.format("%s['\"]", fieldDelimiter)));
        }
        //String res = p.matcher(s).replaceAll(fieldDelimiter).replaceAll("^['\"]+|['\"]+$", "");
        String res = s;
        for(Pattern p : patterns) {
            res = p.matcher(res).replaceAll(fieldDelimiter);
        }
        res = res.replaceAll("^['\"]+|['\"]+$", "");
        return res;
    }
}
