package banana.crawler.dowload.processor;

import java.io.IOException;

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
				float p0 = Float.parseFloat(options.param(0,0).toString());
				float p1 = Float.parseFloat(options.param(1,0).toString());
				return p0 > p1;
			}
		});
		registerHelper("lt", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				float p0 = Float.parseFloat(options.param(0,0).toString());
				float p1 = Float.parseFloat(options.param(1,0).toString());
				return p0 < p1;
			}
		});
		registerHelper("existKey", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String key = options.param(0);
				RuntimeContext runtimeContext = (RuntimeContext) options.context.model();
				return runtimeContext.containsKey(key);
			}
		});
		
	}
	
}
