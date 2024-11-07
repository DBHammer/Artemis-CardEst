package ecnu.dbhammer.utils.datadistribution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.math3.distribution.NormalDistribution;

public class NormalAccessDistribution implements AccessDistribution, Serializable {

	private static final long serialVersionUID = 1L;

	
	private NormalDistribution normalGenerator;
	
	private int begin;
	
	private int end;
	
	private double mean;
	
	private double standardDeviation;
	
	
	public NormalAccessDistribution(int begin, int end) {
		super();
		
		this.begin = begin;
		this.end = end;
		this.mean = (this.end + this.begin) / 2.0d;
		this.standardDeviation = (this.end - this.begin) / 6.0d;
		
		normalGenerator = new NormalDistribution(mean, standardDeviation);
	}

	@Override
	public double getData() {
		
		double temp = normalGenerator.sample();
		
		
		while(temp < this.begin && temp < this.end || temp > this.begin && temp > this.end) {
			temp = normalGenerator.sample();
		}
		
		return temp;
	}
	
	@Override
	public int getDataInteger() {
		return (int)getData();
	}
	

	@Override
	public String toString() {
		return "NormalAccessDistribution [begin=" + begin + ", end=" + end + ", mean=" + mean + ", standardDeviation="
				+ standardDeviation + "]";
	}
}
