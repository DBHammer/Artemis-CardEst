package ecnu.dbhammer.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ObjectUtils.Null;

public class ReplaceAttr {
    private static final Pattern CAST = Pattern.compile("cast");
    private static final Pattern CONCAT = Pattern.compile("concat");
    private static final Pattern COL = Pattern.compile("table_\\d+.col_\\d+");
    private static final Pattern FK = Pattern.compile("table_\\d+.fk_\\d+");
    private static final Pattern PK = Pattern.compile("table_\\d+.primaryKey_\\d+");
    private static final Pattern COMMA_BRACKET = Pattern.compile(",\\)");
    private static final Pattern COMMA = Pattern.compile(",");
    public static String replaceAttr(String text, String attrName) {
        if(text.contains("cast")) {
            if(text.contains("col")) {
                text = COL.matcher(text).replaceAll(attrName + " ");
            } else if(text.contains("fk")) {
                text = FK.matcher(text).replaceAll(attrName + " ");
            } else if(text.contains("primaryKey")) {
                text = PK.matcher(text).replaceAll(attrName + " ");
            }
        } else if(text.contains("concat")) {
            text = COMMA.matcher(text).replaceAll(" ");
            if(text.contains("col")) {
                text = COL.matcher(text).replaceAll(attrName + ",");
            } else if(text.contains("fk")) {
                text = FK.matcher(text).replaceAll(attrName + ",");
            } else if(text.contains("primaryKey")) {
                text = PK.matcher(text).replaceAll(attrName + ",");
            }
            text = COMMA_BRACKET.matcher(text).replaceAll(")");
        }
        return text;
    }

    public static void main(String[] args) {
        String text = "concat(table_8.fk_7)";
        String s = replaceAttr(text, "table_1.fk_5");
        System.out.println(s);
    }
}