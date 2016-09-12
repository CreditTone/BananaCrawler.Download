package banana.crawler.dowload.processor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.http.NameValuePair;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;

import banana.core.modle.ContextModle;
import banana.core.request.HttpRequest;
import banana.core.request.StartContext;
import banana.core.response.Page;

public final class RuntimeContext implements ContextModle {

	private static final Handlebars handlebars = new ExpandHandlebars();

	private Map<String, Object> requestAttribute;

	private Map<String, Object> pageContext;
	
	private Map<String, Object> dataContext;
	
	public static final RuntimeContext create(Page page,StartContext context){
		HttpRequest request = page.getRequest();
		Map<String,Object> pageContext = new HashMap<String,Object>();
		pageContext.put("_url", request.getUrl());
		List<NameValuePair> pair = request.getNameValuePairs();
		for (NameValuePair pr : pair) {
			pageContext.put(pr.getName(), pr.getValue());
		}
		RuntimeContext runtimeContext = new RuntimeContext(request.getAttributes(), pageContext);
		runtimeContext.put("_content", page.getContent());
		return runtimeContext;
	}

	public RuntimeContext(Map<String, Object> requestAttribute, Map<String, Object> pageContext) {
		this.requestAttribute = requestAttribute;
		this.pageContext = pageContext;
		put("_", "");
	}
	
	public String parse(String line) throws IOException {
		return parse(line, null);
	}

	public String parse(String line, Map<String, Object> tempDataContext) throws IOException {
		if (!line.contains("{{")){
			return line;
		}
		Template template = handlebars.compileInline(line);
		if (tempDataContext != null){
			HashMap<String, Object> temp = new HashMap<String, Object>(tempDataContext) {

				@Override
				public Object get(Object key) {
					Object value = super.get(key);
					if (value != null) {
						return value;
					}
					return RuntimeContext.this.get(key);
				}

			};
			return StringEscapeUtils.unescapeHtml(template.apply(temp));
		}
		return StringEscapeUtils.unescapeHtml(template.apply(this));
	}
	
	public void setDataContext(Map<String, Object> dataContext){
		this.dataContext = dataContext;
	}
	
	public void setDataContextNull(){
		this.dataContext = null;
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
