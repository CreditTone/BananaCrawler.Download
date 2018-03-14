package banana.dowloader.processor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.NameValuePair;

import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Options;
import com.github.jknack.handlebars.Template;

import banana.core.ExpandHandlebars;
import banana.core.modle.ContextModle;
import banana.core.request.HttpRequest;
import banana.core.response.Page;
import banana.core.response.StreamResponse;
import banana.dowloader.impl.RemoteTaskContext;

public final class RuntimeContext implements ContextModle {

	private static final ExpandHandlebars handlebars = new ExpandHandlebars();
	
	static{
		handlebars.registerHelper("isNull", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				RuntimeContext runtimeContext = (RuntimeContext) options.context.model();
				Object value = runtimeContext.get(path);
				return value == null;
			}
		});
		handlebars.registerHelper("notNull", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				RuntimeContext runtimeContext = (RuntimeContext) options.context.model();
				Object value = runtimeContext.get(path);
				return value != null;
			}
		});
		handlebars.registerHelper("containString", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				RuntimeContext runtimeContext = (RuntimeContext) options.context.model();
				String content = (String) runtimeContext.get("_content");
				for (int i = 0; i < options.params.length; i++) {
					if (!content.contains(options.param(i).toString())){
						return false;
					}
				}
				return true;
			}
		});
	}
	
	private ContextModle globalContext;
	
	private RemoteTaskContext taskContext;

	private Map<String, Object> requestAttribute;

	private Map<String, Object> pageContext;
	
	private Map<String, Object> dataContext;
	
	protected char filter_index = 'a';
	
	private List<HttpRequest> queue;
	
	public static final String FILTER_PREFIX = "filter_";
	
	public static final RuntimeContext create(Page page,RemoteTaskContext context){
		RuntimeContext runtimeContext = RuntimeContext.create(page.getRequest(), context);
		runtimeContext.put("_owner_url", page.getOwnerUrl());
		runtimeContext.put("_content", page.getContent());
		runtimeContext.put("_status_code", page.getStatus());
		return runtimeContext;
	}
	
	public static final RuntimeContext create(StreamResponse stream,RemoteTaskContext context){
		RuntimeContext runtimeContext = RuntimeContext.create(stream.getRequest(), context);
		runtimeContext.put("_owner_url", stream.getOwnerUrl());
		runtimeContext.put("_status_code", stream.getStatus());
		return runtimeContext;
	}
	
	public static final RuntimeContext create(HttpRequest request, RemoteTaskContext context){
		Map<String,Object> pageContext = new HashMap<String,Object>();
		pageContext.put("_url", request.getUrl());
		List<NameValuePair> pair = request.getNameValuePairs();
		for (NameValuePair pr : pair) {
			pageContext.put(pr.getName(), pr.getValue());
		}
		RuntimeContext runtimeContext = new RuntimeContext(request.getAttributes(), pageContext, context);
		return runtimeContext;
	}

	public RuntimeContext(Map<String, Object> requestAttribute, Map<String, Object> pageContext,RemoteTaskContext taskContext) {
		this.requestAttribute = requestAttribute;
		this.pageContext = pageContext;
		this.taskContext = taskContext;
	}
	
	public List<HttpRequest> getQueue() {
		return queue;
	}

	public void setQueue(List<HttpRequest> queue) {
		this.queue = queue;
	}

	public void putFilterCount(int filterCount) {
		pageContext.put(FILTER_PREFIX + filter_index, filterCount);
		filter_index ++;
	}
	
	public Object parseObject(String line) throws IOException {
		if (line.startsWith("{{") && line.endsWith("}}") && !line.contains(" ")) {
			return get(line.substring(2, line.length() -2));
		}
		return parseString(line);
	}
	
	public Map<String,Object> fillData(Map<String,Object> data) throws IOException {
		Map<String,Object> data2 = new HashMap<String,Object>();
		for (Entry<String,Object> item : data.entrySet()) {
			Object value = item.getValue();
			if (value instanceof Map) {
				value = fillData((Map<String, Object>) value);
			} else if (value instanceof String) {
				value = parseString((String) value);
			}
			data2.put(item.getKey(), value);
		}
		return data2;
	}
	
	@Override
	public String parseString(String line) throws IOException {
		if (!line.contains("{{")){
			return line;
		}
		Template template = handlebars.compileEscapeInline(line);
		return template.apply(this);
	}
	
	public void setDataContext(Map<String, Object> dataContext){
		this.dataContext = dataContext;
	}
	
	public Map<String, Object> getDataContext() {
		return dataContext;
	}

	public void setDataContextNull(){
		this.dataContext = null;
	}
	
	public void copyTo(Map<String,Object> dst){
		if (this.dataContext != null){
			dst.putAll(dataContext);
		}
		if (this.pageContext != null){
			dst.putAll(pageContext);
		}
		if (this.requestAttribute != null){
			dst.putAll(requestAttribute);
		}
	}
	
	@Override
	public int size() {
		return requestAttribute.size() + pageContext.size();
	}

	@Override
	public boolean isEmpty() {
		return requestAttribute.isEmpty() || pageContext.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return requestAttribute.containsKey(key) || pageContext.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return requestAttribute.containsValue(value) || pageContext.containsKey(value);
	}
	
	private Object deepGet(String key) {
		String[] path = key.split("\\.");
		Object value = null;
		for (String stepKey : path) {
			if (value == null) {
				value = get(stepKey);
			}else {
				if (!(value instanceof Map)) {
					return null;
				}
				Map temp = (Map) value;
				value = temp.get(stepKey);
				if (value == null) {
					return null;
				}
			}
		}
		return value;
	}

	@Override
	public Object get(Object key) {
		if (((String) key).contains(".")) {
			return deepGet((String) key);
		}
		Object value = null;
		if (dataContext != null){
			value = dataContext.get(key);
		}
		if (value != null){
			return value;
		}
		value = requestAttribute.get(key);
		if (value != null){
			return value;
		}
		value = pageContext.get(key);
		if (value != null){
			return value;
		}
		return taskContext.get((String) key);
	}

	public <T> T get(Object key, T defaultValue) {
		T value = (T) get(key);
		return value == null?defaultValue:value;
	}

	@Override
	public Object put(String key, Object value) {
		return pageContext.put(key, value);
	}

	@Override
	public Object remove(Object key) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<String> keySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<Object> values() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<java.util.Map.Entry<String, Object>> entrySet() {
		throw new UnsupportedOperationException();
	}
}
