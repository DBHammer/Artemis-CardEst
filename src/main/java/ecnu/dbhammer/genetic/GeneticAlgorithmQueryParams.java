package ecnu.dbhammer.genetic;

import java.util.Arrays;
import java.util.Random;
import java.util.ArrayList;

public class GeneticAlgorithmQueryParams {
    private static final Random random = new Random();
    private static final int POPULATION_SIZE = 50;
    private static final int MAX_GENERATIONS = 10;
    private static final double MUTATION_RATE = 0.1;
    private static final int TOURNAMENT_SIZE = 5;

    // 定义目标过滤比范围
    private static final double[] FILTER_RANGES = { 0.4, 0.5 }; // 比如过滤比范围在10%到50%

    // 生成随机过滤比
    private static double getRandomFilterRatio() {
        return FILTER_RANGES[0] + (FILTER_RANGES[1] - FILTER_RANGES[0]) * random.nextDouble();
    }

    // 初始化种群
    private static double[][] initializePopulation(int numNodes) {
        double[][] population = new double[POPULATION_SIZE][numNodes];
        for (int i = 0; i < POPULATION_SIZE; i++) {
            for (int j = 0; j < numNodes; j++) {
                population[i][j] = getRandomFilterRatio();
            }
        }
        return population;
    }

    // 适应度函数（示例适应度计算，实际应用中需要具体定义）
    private static double calculateFitness(double[] individual) {
        // 示例适应度为过滤比的方差，越均匀越好
        double mean = Arrays.stream(individual).average().orElse(0);
        double variance = Arrays.stream(individual).map(r -> (r - mean) * (r - mean)).average().orElse(0);
        return 1.0 / variance; // 方差越小适应度越高
    }

    // 选择操作（锦标赛选择）
    private static double[] tournamentSelection(double[][] population) {
        double[] best = null;
        for (int i = 0; i < TOURNAMENT_SIZE; i++) {
            double[] individual = population[random.nextInt(POPULATION_SIZE)];
            if (best == null || calculateFitness(individual) > calculateFitness(best)) {
                best = individual;
            }
        }
        return best;
    }

    // 交叉操作
    private static double[] crossover(double[] parent1, double[] parent2) {
        int crossoverPoint = random.nextInt(parent1.length);
        double[] child = new double[parent1.length];
        for (int i = 0; i < parent1.length; i++) {
            if (i < crossoverPoint) {
                child[i] = parent1[i];
            } else {
                child[i] = parent2[i];
            }
        }
        return child;
    }

    // 变异操作
    private static void mutate(double[] individual) {
        for (int i = 0; i < individual.length; i++) {
            if (random.nextDouble() < MUTATION_RATE) {
                individual[i] = getRandomFilterRatio();
            }
        }
    }

    public ArrayList<double[]> combination(int filterNode) {
        double[][] population = initializePopulation(filterNode);
        ArrayList<double[]> res = new ArrayList<>();
        for (int generation = 0; generation < MAX_GENERATIONS; generation++) {
            double[][] newPopulation = new double[POPULATION_SIZE][];
            for (int i = 0; i < POPULATION_SIZE; i++) {
                double[] parent1 = tournamentSelection(population);
                double[] parent2 = tournamentSelection(population);
                double[] child = crossover(parent1, parent2);
                mutate(child);
                newPopulation[i] = child;
            }
            population = newPopulation;
            res.add(population[0]);
        }
        return res;
    }
}