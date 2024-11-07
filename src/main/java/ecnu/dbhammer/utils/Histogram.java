package ecnu.dbhammer.utils;



import java.util.List;

public class Histogram {

    private List<HistogramItem> items = null;

    private double[] cumulativeProbabilities = null;

    public Histogram(List<HistogramItem> items) throws Exception {
        super();
        this.items = items;
        if (items == null || items.size() == 0) {
            throw new Exception("构造直方图配置信息时出错，items为空！");
        }

        cumulativeProbabilities = new double[items.size()];
        cumulativeProbabilities[0] = items.get(0).getRatio();
        for (int i = 1; i < items.size(); i++) {
            cumulativeProbabilities[i] = cumulativeProbabilities[i - 1] + items.get(i).getRatio();
        }
    }

    // 未相关配置项的概率和不为1，则需做-1判断
    //随机生成一个值，看这个值落到哪个item上，然后这个item上的最大值和最小值之间随机一个数
    public int getRandomIntValue() {
        double randomValue = Math.random() - 0.000000001;
        for (int i = 0; i < cumulativeProbabilities.length; i++) {
            if (randomValue < cumulativeProbabilities[i]) {
                HistogramItem histogramItem = items.get(i);
                int minValue = (int)histogramItem.getMinValue();
                int maxValue = (int)histogramItem.getMaxValue();
                return (int)(Math.random() * (maxValue - minValue + 1)) + minValue;
            }
        }
        return -1;
    }

    public double getProbabilitySum() {
        return cumulativeProbabilities[cumulativeProbabilities.length - 1];//计算所有items的概率
    }

    @Override
    public String toString() {
        return "Histogram [items=" + items + "]";
    }

}
