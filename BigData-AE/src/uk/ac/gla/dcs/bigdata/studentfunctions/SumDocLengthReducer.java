package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;

/**
 * @author zoltan
 * Reduce function, returning the sum of integers.
 */
public class SumDocLengthReducer implements ReduceFunction<Integer> {
	
	private static final long serialVersionUID = 6475166483071609772L;

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		// TODO Auto-generated method stub
		return v1 + v2;
	}

}
