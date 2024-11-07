package ecnu.dbhammer.query.gene;

import org.uma.jmetal.algorithm.Algorithm;
import org.uma.jmetal.algorithm.examples.AlgorithmRunner;
import org.uma.jmetal.algorithm.multiobjective.nsgaii.NSGAIIBuilder;
import org.uma.jmetal.algorithm.singleobjective.geneticalgorithm.GeneticAlgorithmBuilder;
import org.uma.jmetal.operator.crossover.CrossoverOperator;
import org.uma.jmetal.operator.mutation.MutationOperator;
import org.uma.jmetal.operator.crossover.impl.SBXCrossover;
import org.uma.jmetal.operator.mutation.impl.PolynomialMutation;
import org.uma.jmetal.operator.selection.SelectionOperator;
import org.uma.jmetal.operator.selection.impl.BinaryTournamentSelection;
import org.uma.jmetal.solution.doublesolution.DoubleSolution;
import org.uma.jmetal.util.JMetalLogger;
import org.uma.jmetal.util.bounds.Bounds;
import org.uma.jmetal.util.comparator.RankingAndCrowdingDistanceComparator;

import java.util.ArrayList;
import java.util.List;

/**
 * NSGAIIQueryOptimizer 是一个用于优化查询参数的类，支持单目标和多目标优化。
 */
public class NSGAIIQueryOptimizer {
    private final ParameterInstantiationProblem problem;
    private final int validSolutionThreshold = 1;

    // 基本配置
    private final int populationSize = 30;
    private final int maxEvaluations = 80;

    // 交叉算子配置
    private final double crossoverProbability = 0.9;
    private final double crossoverDistributionIndex = 20.0;
    private final CrossoverOperator<DoubleSolution> crossover;

    // 变异算子配置
    private final double mutationProbability;
    private final double mutationDistributionIndex = 20.0;
    private final MutationOperator<DoubleSolution> mutation;

    // 选择算子配置
    private final SelectionOperator<List<DoubleSolution>, DoubleSolution> selection;

    /**
     * 构造函数，初始化优化问题和算法参数。
     *
     * @param problem 参数实例化问题对象
     */
    public NSGAIIQueryOptimizer(ParameterInstantiationProblem problem) {
        this.problem = problem;
        this.mutationProbability = 1.0 / problem.getNumberOfVariables();
        this.crossover = new SBXCrossover(crossoverProbability, crossoverDistributionIndex);
        this.mutation = new PolynomialMutation(mutationProbability, mutationDistributionIndex);
        this.selection = new BinaryTournamentSelection<>(new RankingAndCrowdingDistanceComparator<>());
    }

    /**
     * 运行优化器，根据历史解集的大小选择单目标或多目标优化算法。
     *
     * @return 最优解的变量列表
     */
    public List<Double> runOptimizer() {
        if (problem.getHistoricalSolutions().size() < validSolutionThreshold) {
            return runSingleObjectiveGA();
        } else {
            return runNSGAII();
        }
    }

    /**
     * 更新历史解集，添加新的解。
     *
     * @param solution 新的解
     */
    public void updateHistorySolution(DoubleSolution solution) {
        problem.addSolution(solution);
    }

    /**
     * 运行单目标遗传算法进行优化。
     *
     * @return 最优解的变量列表
     */
    private List<Double> runSingleObjectiveGA() {
        GeneticAlgorithmBuilder<DoubleSolution> builder = new GeneticAlgorithmBuilder<>(
                problem,
                crossover,
                mutation)
                .setPopulationSize(populationSize)
                .setMaxEvaluations(maxEvaluations)
                .setSelectionOperator(selection);

        System.out.println("Starting the single-objective GA algorithm...");
        Algorithm<DoubleSolution> algorithm = builder.build();
        AlgorithmRunner algorithmRunner = new AlgorithmRunner.Executor(algorithm).execute();

        long computingTime = algorithmRunner.getComputingTime();
        DoubleSolution solution = algorithm.result();

        if (solution == null) {
            System.out.println("No solution was found.");
            return new ArrayList<>();
        } else {
            System.out.println("Best solution found:");
            double objectiveValue = solution.objectives()[0];
            if (Double.isNaN(objectiveValue)) {
                JMetalLogger.logger.severe("NaN found in objective value");
            } else {
                System.out.println("Objective value: " + objectiveValue);
            }
            updateHistorySolution(solution);
        }
        System.out.println("Single Objective GA total computation time: " + computingTime + "ms");
        return solution.variables();
    }

    /**
     * 运行 NSGA-II 算法进行多目标优化。
     *
     * @return 最优解的变量列表
     */
    public List<Double> runNSGAII() {
        System.out.println("Starting the NSGAII dual-objective GA algorithm...");
        NSGAIIBuilder<DoubleSolution> builder = new NSGAIIBuilder<>(problem, crossover, mutation, populationSize)
                .setMaxEvaluations(maxEvaluations);
        Algorithm<List<DoubleSolution>> algorithm = builder.build();
        AlgorithmRunner algorithmRunner = new AlgorithmRunner.Executor(algorithm).execute();

        long computingTime = algorithmRunner.getComputingTime();
        List<DoubleSolution> solutions = algorithm.result();

        System.out.println("NSGA-II total computation time: " + computingTime + "ms");
        for (DoubleSolution solution : solutions) {
            System.out.println(
                    "Solution objectives: F1 = " + solution.objectives()[0] + ", F2 = " + solution.objectives()[1]);
        }

        // 根据目标值选择最佳解
        DoubleSolution bestSolution = null;
        double minObjective0 = Double.POSITIVE_INFINITY;
        double minObjective1 = Double.POSITIVE_INFINITY;
        for (DoubleSolution solution : solutions) {
            double objective0 = solution.objectives()[0];
            double objective1 = solution.objectives()[1];
            if (objective0 < minObjective0 || (objective0 == minObjective0 && objective1 < minObjective1)) {
                minObjective0 = objective0;
                minObjective1 = objective1;
                bestSolution = solution;
            }
        }

        if (bestSolution != null) {
            System.out.println("Best solution found:");
            for (int i = 0; i < problem.numberOfVariables(); i++) {
                System.out.println("Variable " + i + ": " + bestSolution.variables().get(i));
            }
            System.out.println("Objective values: F1 = " + bestSolution.objectives()[0]
                    + ", F2 = " + bestSolution.objectives()[1]);
            updateHistorySolution(bestSolution);
            return new ArrayList<>(bestSolution.variables());
        } else {
            System.out.println("No solution found.");
            return new ArrayList<>();
        }
    }

    /**
     * 获取问题的变量范围列表。
     *
     * @return 变量范围的列表
     */
    public List<Bounds<Double>> getVariableBounds() {
        return problem.getVariableBounds();
    }
}