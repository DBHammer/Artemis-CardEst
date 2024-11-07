package ecnu.dbhammer.utils.datadistribution;

import java.io.Serializable;

import org.apache.commons.math3.distribution.ZipfDistribution;


public class ZipfAccessDistribution implements AccessDistribution, Serializable {

	private static final long serialVersionUID = 1L;

	
	private ZipfDistribution zipfGenerator;

	private int begin;
	
	private int end;
	
	
	private double skew;
	
	public ZipfAccessDistribution(int begin, int end, double skew) {
		super();
		this.begin = begin;
		this.end = end;
		this.skew = skew;
		
		zipfGenerator = new ZipfDistribution((int)(this.end-this.begin), skew);
	}

	
	@Override
	public double getData() {
		 return getDataInteger();
	}
	
	
	@Override
	public int getDataInteger() {
		return zipfGenerator.sample() + this.begin - 1;
	}
}
