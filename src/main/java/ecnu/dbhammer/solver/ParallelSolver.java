package ecnu.dbhammer.solver;

/**
 * @author xiangzhaokun
 * @ClassName ParallelSolver.java
 * @Description 多线程的求解器测试类
 * @createTime 2022年04月12日 15:59:00
 */

import com.google.ortools.Loader;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ParallelSolver {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        long start, end;
        start = System.currentTimeMillis();
        Loader.loadNativeLibraries();
        end = System.currentTimeMillis();

        System.out.println("加载类的时间" + (end - start));


//        start = System.currentTimeMillis();
//        computing(1, 200000);
//        end = System.currentTimeMillis();
//        System.out.println("computing times : " + (end - start));
//
//
//        start = System.currentTimeMillis();
//        computing(1, 200000);
//        end = System.currentTimeMillis();
//        System.out.println("computing times : " + (end - start));

        start = System.currentTimeMillis();
        int amountData = 16000000;
        int nThread = 5000;
        ExecutorService service = Executors.newFixedThreadPool(nThread);

        ComputingTask[] computingTasks = new ComputingTask[nThread];
        int step = (int)Math.ceil(amountData*1.0/nThread);
        int n = 0;
        for (int i = 1; i <= amountData; i += step) {
            computingTasks[n++] = new ComputingTask(i, i + step-1);
        }
        Future<List<List<Integer>>>[] F = new Future[nThread];
        for (int i = 0; i < n; i++) {
            F[i] = service.submit(computingTasks[i]);
        }

        List<List<Integer>> ans = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            for(List<Integer> row : F[i].get()){
                ans.add(row);
            }
        }
        end = System.currentTimeMillis();
        System.out.println("parallel computing times : " + (end - start));
        System.out.println(ans.size());


        service.shutdown();



    }

    private static List<List<Integer>> computing(Integer start, Integer end) {
        return SolverTest.solveTask(start, end);
    }

    static class ComputingTask implements Callable<List<List<Integer>>> {
        int start, end;

        public ComputingTask(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public List<List<Integer>> call() throws Exception {
            return SolverTest2.solveTask(start, end);
        }
    }
}
