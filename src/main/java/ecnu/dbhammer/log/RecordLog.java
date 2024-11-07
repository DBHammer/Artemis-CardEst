package ecnu.dbhammer.log;

import ecnu.dbhammer.constant.LogLevelConstant;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
/**
 * @author xiangzhaokun
 * @ClassName RecordLog.java
 * @Description 记录日志
 * @createTime 2021年03月13日 21:07:00
 */
public class RecordLog {

    static Logger logger = Logger.getLogger(RecordLog.class.getName());

    /**
     * 日志长度,用于规范日志，对齐
     */
    private static int logRecordLength = 90;

    /**
     * log4j配置文件路径
     */
    private static String log4jConfigPath = "./src/main/resources/config/log4j.properties";

    static
    {
        PropertyConfigurator.configure(log4jConfigPath);
    }

    public static void recordLog(String logLevel, String logInfo, Object... args)
    {
        // RecordLog.FunctionRecord(getRecordMetadata(), String.format(logInfo, args), logLevel);
    }

    private static void FunctionRecord(String moduleInfo, String stateInfo, String stateLevel)
    {
        StringBuilder tmp = new StringBuilder(moduleInfo);
        for (int i = tmp.length(); i <= logRecordLength; i++)
        {
            tmp.append(" ");
        }
        moduleInfo = tmp.toString();
        String message = moduleInfo + stateInfo;
        if (stateLevel.equals(LogLevelConstant.INFO))
        {
            logger.info(message);
        }
        else if (stateLevel.equals(LogLevelConstant.ERROR))
        {
            logger.error(message);
        }
        else if (stateLevel.equals(LogLevelConstant.WARN))
        {
            logger.warn(message);
        }
        else if (stateLevel.equals(LogLevelConstant.DEBUG))
        {
            logger.debug(message);
        }
    }

    public static String getRecordMetadata()
    {
        StackTraceElement element = Thread.currentThread().getStackTrace()[3];
        return String.format("%s->%s:%d", element.getClassName(), element.getMethodName(), element.getLineNumber());
    }

    public static void main(String[] args) {
        RecordLog.recordLog(LogLevelConstant.INFO,"日志test");
    }
}
