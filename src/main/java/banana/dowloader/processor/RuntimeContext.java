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
import banana.core.request.StartContext;
import banana.core.response.Page;

public final class RuntimeContext implements ContextModle {

	private static final ExpandHandlebars handlebars = new ExpandHandlebars();
	
	static{
		handlebars.registerHelper("existKey", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				RuntimeContext runtimeContext = (RuntimeContext) options.context.model();
				return runtimeContext.existKey(path);
			}
		});
		handlebars.registerHelper("isEmpty", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String path = options.param(0);
				RuntimeContext runtimeContext = (RuntimeContext) options.context.model();
				return !runtimeContext.existKey(path);
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

	private Map<String, Object> requestAttribute;

	private Map<String, Object> pageContext;
	
	private Map<String, Object> dataContext;
	
	public static final RuntimeContext create(Page page,StartContext context){
		RuntimeContext runtimeContext = RuntimeContext.create(page.getRequest(), context);
		runtimeContext.put("_content", page.getContent());
		runtimeContext.put("_status_code", page.getStatus());
		return runtimeContext;
	}
	
	public static final RuntimeContext create(HttpRequest request,StartContext context){
		Map<String,Object> pageContext = new HashMap<String,Object>();
		pageContext.put("_url", request.getUrl());
		List<NameValuePair> pair = request.getNameValuePairs();
		for (NameValuePair pr : pair) {
			pageContext.put(pr.getName(), pr.getValue());
		}
		RuntimeContext runtimeContext = new RuntimeContext(request.getAttributes(), pageContext);
		return runtimeContext;
	}

	public RuntimeContext(Map<String, Object> requestAttribute, Map<String, Object> pageContext) {
		this.requestAttribute = requestAttribute;
		this.pageContext = pageContext;
	}
	
	public boolean existKey(String path) {
		String[] keys = path.split("\\.");
		Object value = get(keys[0]);
		for (int i = 1; i < keys.length; i++) {
			if (value == null ){
				break;
			}
			if (keys[i].startsWith("[")){
				int index = Integer.parseInt(keys[i].substring(1, keys[i].length()-1));
				value = ((List<Object>)value).get(index);
			}else{
				value = ((Map<String,Object>)value).get(keys[i]);
			}
		}
		if (value != null){
			return !value.equals("");
		}
		return false;
	}
	
	public String parse(String line) throws IOException {
		return parse(line, null);
	}

	public String parse(String line, Map<String, Object> tempDataContext) throws IOException {
		if (!line.contains("{{")){
			return line;
		}
		Template template = handlebars.compileEscapeInline(line);
		if (tempDataContext != null){
			setDataContext(tempDataContext);
		}
		String ret = template.apply(this);
		return ret;
	}
	
	public void setDataContext(Map<String, Object> dataContext){
		this.dataContext = dataContext;
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

	@Override
	public Object get(Object key) {
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
		return pageContext.get(key);
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
