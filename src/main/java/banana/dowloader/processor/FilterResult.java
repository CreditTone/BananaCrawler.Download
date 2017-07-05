package banana.dowloader.processor;

import com.alibaba.fastjson.JSON;

public class FilterResult {

	private int filterCount;
	
	private JSON result;

	public int getFilterCount() {
		return filterCount;
	}

	public void setFilterCount(int filterCount) {
		this.filterCount = filterCount;
	}

	public JSON getResult() {
		return result;
	}

	public void setResult(JSON result) {
		this.result = result;
	}
	
}
