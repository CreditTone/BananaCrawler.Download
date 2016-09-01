package banana.crawler.dowload.processor;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Template;

public final class RuntimeContext implements Map<String,Object>{
	
	private static final Handlebars handlebars = new ExpandHandlebars();
	
	private Map<String,Object> requestAttribute;
	
	private Map<String,Object> pageContext;

	public RuntimeContext(Map<String, Object> requestAttribute, Map<String, Object> pageContext) {
		this.requestAttribute = requestAttribute;
		this.pageContext = pageContext;
	}
	
	public String parse(String line) throws IOException{
		Template template = handlebars.compileInline(line);
		return template.apply(this);
	}
	
	public String parse(String line,Map<String,Object> temp) throws IOException{
		Template template = handlebars.compileInline(line);
		HashMap<String,Object> temp2 = new HashMap<String,Object>(temp){

			@Override
			public Object get(Object key) {
				Object value = super.get(key);
				if (value != null){
					return value;
				}
				return RuntimeContext.this.get(key);
			}
			
		};
		return template.apply(temp2);
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
		Object value = requestAttribute.get(key);
		if (value == null){
			value = pageContext.get(key);
		}
		return value;
	}

	@Override
	public Object put(String key, Object value) {
		throw new UnsupportedOperationException();
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
