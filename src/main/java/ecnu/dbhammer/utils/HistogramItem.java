package ecnu.dbhammer.utils;



public class HistogramItem {

    private double minValue;//最小值
    private double maxValue;//最大值
    private double ratio;//比例

    public HistogramItem(double minValue, double maxValue, double ratio) {
        super();
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.ratio = ratio;
    }

    public double getMinValue() {
        return minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public double getRatio() {
        return ratio;
    }

    @Override
    public String toString() {
        return "HistogramItem [minValue=" + minValue + ", maxValue=" + maxValue + ", ratio=" + ratio + "]";
    }

}
