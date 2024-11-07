package ecnu.dbhammer.solver;


import ecnu.dbhammer.configuration.Configurations;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author xiangzhaokun
 * @ClassName ThreadPools.java
 * @Description 线程池中的线程数要根据数据量、机器的配置调整，不能太少、也不能太多
 * @createTime 2022年04月12日 21:27:00
 */
public class ThreadPools {
    private static ExecutorService service = Executors.newFixedThreadPool(Configurations.getSolverThread());

    public static ExecutorService getService() {
        return service;
    }

    public static void down(){
        service.shutdown();
    }
}
