package ecnu.dbhammer.utils.datadistribution;

import java.io.Serializable;
import java.util.Random;


public class UniformAccessDistribution implements AccessDistribution, Serializable {
	
	private static final long serialVersionUID = 1L;

	private Random random;
	
	private int begin;
	
	private int end;

	public UniformAccessDistribution(int begin, int end) {
		super();
		random = new Random();
		this.begin = begin;
		this.end = end;
	}
	
	
	public double getData() {
		return random.nextInt((int)(this.end - this.begin)) + this.begin + random.nextDouble();
	}
	
	
	public int getDataInteger() {
		return random.nextInt((int)(this.end - this.begin)) + this.begin;
	}
}
