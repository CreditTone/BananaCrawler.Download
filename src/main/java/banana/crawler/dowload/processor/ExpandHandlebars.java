package banana.crawler.dowload.processor;

import java.io.IOException;

import org.apache.commons.lang.StringEscapeUtils;

import com.github.jknack.handlebars.Handlebars;
import com.github.jknack.handlebars.Helper;
import com.github.jknack.handlebars.Options;

public class ExpandHandlebars extends Handlebars {

	public ExpandHandlebars() {
		registerHelper("add", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				int sum = 0;
				int p0 = 0;
				for (int i = 0; i < options.params.length; i++) {
					p0 = Integer.parseInt(options.param(i).toString());
					sum += p0;
				}
				return sum;
			}
		});
		registerHelper("sub", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				int p0 = options.param(0);
				for (int i = 1; i < options.params.length; i++) {
					int p = Integer.parseInt(options.param(i).toString());
					p0 -= p;
				}
				return p0;
			}
		});
		registerHelper("multiply", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				int product = Integer.parseInt(options.param(0).toString());;
				int p0 ;
				for (int i = 1; i < options.params.length; i++) {
					p0 = Integer.parseInt(options.param(i).toString());
					product *= p0;
				}
				return product;
			}
		});
		registerHelper("gt", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				float p0 = Float.parseFloat(((Object)options.param(0)).toString());
				float p1 = Float.parseFloat(((Object)options.param(1)).toString());
				return p0 > p1;
			}
		});
		registerHelper("lt", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				float p0 = Float.parseFloat(((Object)options.param(0)).toString());
				float p1 = Float.parseFloat(((Object)options.param(1)).toString());
				return p0 < p1;
			}
		});
		registerHelper("eq", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				Object s1 = options.param(0);
				Object s2 = options.param(1);
				return s1.equals(s2);
			}
		});
		registerHelper("existKey", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String key = options.param(0);
				RuntimeContext runtimeContext = (RuntimeContext) options.context.model();
				return runtimeContext.containsKey(key);
			}
		});
		registerHelper("fixKey", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String url = options.param(0);
				for (int i = 1; i < options.params.length; i++) {
					url = fixKey(url, (String)options.param(i));
				}
				return url;
			}
		});
		
	}
	
	private static String fixKey(String url,String key) {
		String regex = key + "=[\\w]+(&|\\s*)";
		url = url.replaceAll(regex, "");
		return url;
	}
	
}
