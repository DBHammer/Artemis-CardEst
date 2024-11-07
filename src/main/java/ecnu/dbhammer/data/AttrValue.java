package ecnu.dbhammer.data;

import org.apache.commons.lang3.NotImplementedException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * @author xiangzhaokun
 * @ClassName AttrValue.java
 * @Description 属性值，里面包括该值的类型以及实际的Object对象
 * @createTime 2021年11月29日 15:39:00
 */
public class AttrValue implements Serializable {
    public final DataType type;
    public final Object value;

    public AttrValue(DataType type, Object value) {
        super();
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        return "AttrValue{" +
                "type=" + type +
                ", value=" + value +
                '}';
    }

    public static String tranString(AttrValue attrValue) {
        Object value = attrValue.value;
        return String.valueOf(value);
    }

    public static AttrValue parse(DataType type, String value) {
        switch (type) {
            case INTEGER:
                return new AttrValue(DataType.INTEGER, Integer.parseInt(value));
            case LONG:
                return new AttrValue(DataType.LONG, Long.parseLong(value));
            case FLOAT:
                return new AttrValue(DataType.FLOAT, Float.parseFloat(value));
            case DOUBLE:
                return new AttrValue(DataType.DOUBLE, Double.parseDouble(value));
            case DECIMAL:
                return new AttrValue(DataType.DECIMAL, new BigDecimal(value));
            case VARCHAR:
                return new AttrValue(DataType.VARCHAR, value);
            case TIMESTAMP:
                return new AttrValue(DataType.TIMESTAMP, Timestamp.valueOf(value));
            case BOOL:
                return new AttrValue(DataType.BOOL, Boolean.parseBoolean(value));
            default:
                throw new RuntimeException("暂不支持该类型 " + type.name());
        }
    }

    public boolean Compare(AttrValue v2, String comp) {
        switch (type) {
            case INTEGER:
                if (comp.equals("<")) {
                    return (int) this.value < (int) v2.value;
                } else if (comp.equals("<=")) {
                    return (int) this.value <= (int) v2.value;
                } else if (comp.equals(">")) {
                    return (int) this.value > (int) v2.value;
                } else if (comp.equals(">=")) {
                    return (int) this.value >= (int) v2.value;
                } else if (comp.equals("==")) {
                    return (int) this.value == (int) v2.value;
                }
            case LONG:
                if (comp.equals("<")) {
                    return (long) this.value < (long) v2.value;
                } else if (comp.equals("<=")) {
                    return (long) this.value <= (long) v2.value;
                } else if (comp.equals(">")) {
                    return (long) this.value > (long) v2.value;
                } else if (comp.equals(">=")) {
                    return (long) this.value >= (long) v2.value;
                } else if (comp.equals("==")) {
                    return (long) this.value == (long) v2.value;
                }
            case FLOAT:
                if (comp.equals("<")) {
                    return (float) this.value < (float) v2.value;
                } else if (comp.equals("<=")) {
                    return (float) this.value <= (float) v2.value;
                } else if (comp.equals(">")) {
                    return (float) this.value > (float) v2.value;
                } else if (comp.equals(">=")) {
                    return (float) this.value >= (float) v2.value;
                } else if (comp.equals("==")) {
                    return (float) this.value == (float) v2.value;
                } // TODO可能需要改成float的非精确比较
            case DOUBLE:
                if (comp.equals("<")) {
                    return (double) this.value < (double) v2.value;
                } else if (comp.equals("<=")) {
                    return (double) this.value <= (double) v2.value;
                } else if (comp.equals(">")) {
                    return (double) this.value > (double) v2.value;
                } else if (comp.equals(">=")) {
                    return (double) this.value >= (double) v2.value;
                } else if (comp.equals("==")) {
                    return (double) this.value == (double) v2.value;
                }
            case DECIMAL:
                if (comp.equals("<")) {
                    return ((BigDecimal) this.value).compareTo((BigDecimal) v2.value) < 0;
                } else if (comp.equals("<=")) {
                    return ((BigDecimal) this.value).compareTo((BigDecimal) v2.value) <= 0;
                } else if (comp.equals(">")) {
                    return ((BigDecimal) this.value).compareTo((BigDecimal) v2.value) > 0;
                } else if (comp.equals(">=")) {
                    return ((BigDecimal) this.value).compareTo((BigDecimal) v2.value) >= 0;
                } else if (comp.equals("==")) {
                    return ((BigDecimal) this.value).compareTo((BigDecimal) v2.value) == 0;
                }
            case TIMESTAMP:
                if (comp.equals("<")) {
                    return ((Timestamp) this.value).getTime() < ((Timestamp) v2.value).getTime();
                } else if (comp.equals("<=")) {
                    return ((Timestamp) this.value).getTime() <= ((Timestamp) v2.value).getTime();
                } else if (comp.equals(">")) {
                    return ((Timestamp) this.value).getTime() > ((Timestamp) v2.value).getTime();
                } else if (comp.equals(">=")) {
                    return ((Timestamp) this.value).getTime() >= ((Timestamp) v2.value).getTime();
                } else if (comp.equals("==")) {
                    return ((Timestamp) this.value).getTime() == ((Timestamp) v2.value).getTime();
                }
            case VARCHAR:
                if (comp.equals("=")) {
                    return (this.value).equals(v2.value);
                } else if (comp.equals("!=")) {
                    return !(this.value).equals(v2.value);
                }
            case BLOB:
            case BOOL:

            default:
                throw new NotImplementedException();
        }
    }
}
