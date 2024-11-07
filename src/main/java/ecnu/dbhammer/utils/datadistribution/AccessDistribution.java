package ecnu.dbhammer.utils.datadistribution;

import org.dom4j.Element;

public interface AccessDistribution {
	
	public double getData();
	
	
	
	public int getDataInteger();
	
	
	public static AccessDistribution getAccessDistribution(int type, int begin, int end, int skew) {
		
		if(begin >= end)
			throw new RuntimeException("begin must be less than end, begin < end!");
		
		switch(type) {
		case 0:return new UniformAccessDistribution(begin, end);
		case 1:return new NormalAccessDistribution(begin, end);
		case 2:return new ZipfAccessDistribution(begin, end, skew);
		}
		return null;
	}
	
	
	
}
