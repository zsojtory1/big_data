package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.HashMap;

import org.apache.spark.util.AccumulatorV2;

public class HashMapAccumulator extends AccumulatorV2<String, HashMap<String, Integer>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6601780529055284230L;
	private HashMap<String, Integer> outputHashMap;
	
	public HashMapAccumulator() {
		this.outputHashMap = new HashMap<>();
	}
	
	@Override
	public void add(String v) {
		// TODO Auto-generated method stub
		this.outputHashMap.merge(v, 1, (a, b) -> (a + b));
	}

	@Override
	public AccumulatorV2<String, HashMap<String, Integer>> copy() {
		// TODO Auto-generated method stub
		HashMapAccumulator copy = new HashMapAccumulator();
		copy.merge(this);
		return copy;
	}

	@Override
	public boolean isZero() {
		// TODO Auto-generated method stub
		return outputHashMap.size() == 0;
	}

	@Override
	public void merge(AccumulatorV2<String, HashMap<String, Integer>> other) {
		// TODO Auto-generated method stub
		other.value().forEach((key, value) -> {
            this.outputHashMap.merge(key, value, (oldValue, newValue) -> oldValue + newValue);
		});
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		this.outputHashMap = new HashMap<>();
	}

	@Override
	public HashMap<String, Integer> value() {
		// TODO Auto-generated method stub
		return this.outputHashMap;
	}

}
