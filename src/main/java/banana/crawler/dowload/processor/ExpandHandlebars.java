package banana.crawler.dowload.processor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

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
				if (!url.contains("?")){
					return url;
				}
				String[] urlData  = url.split("\\?");
				String baseUrl = urlData[0];
				String querys  = urlData[1];
				List<NameValuePair> pair = URLEncodedUtils.parse(querys, Charset.defaultCharset());
				for (int i = 1; i < options.params.length; i++) {
					for (NameValuePair nvPair : pair) {
						if (nvPair.getName().equals(options.param(i))){
							pair.remove(nvPair);
							break;
						}
					}
				}
				if (!pair.isEmpty()){
					baseUrl += "?";
					Iterator<NameValuePair> iter = pair.iterator();
					NameValuePair nvPair = null;
					while(iter.hasNext()){
						nvPair = iter.next();
						baseUrl += nvPair.getName() + "=" + nvPair.getValue();
						if (iter.hasNext()){
							baseUrl += "&";
						}
					}
				}
				return baseUrl;
			}
		});
		registerHelper("contains", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				String content = options.param(0);
				for (int i = 1; i < options.params.length; i++) {
					if (!content.contains(options.param(i).toString())){
						return false;
					}
				}
				return true;
			}
		});
		registerHelper("containString", new Helper<Object>() {

			public Object apply(Object context, Options options) throws IOException {
				RuntimeContext runtimeContext = (RuntimeContext) options.context.model();
				String content = (String) runtimeContext.get("_content");
				for (int i = 1; i < options.params.length; i++) {
					if (!content.contains(options.param(i).toString())){
						return false;
					}
				}
				return true;
			}
		});
	}
	
}
