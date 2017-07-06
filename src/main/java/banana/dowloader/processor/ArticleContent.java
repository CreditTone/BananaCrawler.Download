package banana.dowloader.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ArticleContent {
	
	private String baseUrl;
	
	private String html;
	
	private String article_tag;
	
	private StringBuilder article = new StringBuilder();
	
	private String lastAppend ;
	
	private List<ArticleUrl> articleUrls = new ArrayList<>();
	
	public ArticleContent(String url,String html, String article_tag) {
		this.baseUrl = url.substring(0, url.indexOf("/", 7));
		this.html = html;
		this.article_tag = article_tag;
		Document doc = Jsoup.parse(html);
		Element element = doc.select(article_tag).first();
		if (element == null){
			System.err.println(article_tag + " 未找到");
			return;
		}
		toExtract(element);
	}

	public void toExtract(Element element) {
		if (element != null){
			Elements elements = element.children();
			for (int i = 0; i < elements.size(); i++) {
				Element node = element.child(i);
				String nodeName = node.nodeName();
				if ("p".equals(nodeName) || "br".equals(nodeName)){
					if (lastAppend != null && !"\n".equals(lastAppend)){
						article.append("\n");
						lastAppend = "\n";
					}
				}else if ("img".equals(nodeName)){
					if (!"\n".equals(lastAppend)){
						article.append("\n");
					}
					appendUrl(node.attr("src"));
					lastAppend = node.attr("src");
				}
				if (!node.children().isEmpty()){
					if (isParagraph(node)){
						article.append(node.text());
						lastAppend = node.text();
					}else{
						toExtract(node);
					}
				}else{
					article.append(node.text());
					lastAppend = node.text();
				}
			}
		}
	}
	
	private boolean isParagraph(Element node){
		boolean imgEmpty = node.getElementsByTag("img").isEmpty();
		return imgEmpty;
	}

	public String getHtml() {
		return html;
	}

	public void setHtml(String html) {
		this.html = html;
	}

	public String getArticle_tag() {
		return article_tag;
	}

	public void setArticle_tag(String article_tag) {
		this.article_tag = article_tag;
	}
	
	public void appendUrl(String url){
		if (url.startsWith("/")){
			url = baseUrl+url;
		}
		ArticleUrl articleUrl = new ArticleUrl(url);
		this.article.append(articleUrl.getLocalPath());
		articleUrls.add(articleUrl);
	}

	public String getArticle() {
		String cont = article.toString();
		while(cont.startsWith("\n")){
			cont = cont.substring(1, cont.length());
		}
		return cont;
	}
	
	public List<ArticleUrl> getArticleUrls() {
		return articleUrls;
	}

	public void setArticleUrls(List<ArticleUrl> articleUrls) {
		this.articleUrls = articleUrls;
	}

	public class ArticleUrl {
		
		private String imageUrl;
		
		private String localPath;
		
		public ArticleUrl(String imageUrl) {
			this.imageUrl = imageUrl;
			String host = baseUrl.split("//")[1];
			String suffix = ".jpg";
			if (imageUrl.contains(".png")){
				suffix = ".png";
			}else if(imageUrl.contains(".gif")){
				suffix = ".gif";
			}else if(imageUrl.contains(".jpeg")){
				suffix = ".jpeg";
			}
			this.localPath = host + "/" + UUID.randomUUID().toString() + suffix;
			System.out.println(imageUrl + "=" + localPath);
		}
		
		public String getImageUrl() {
			return imageUrl;
		}

		public void setImageUrl(String imageUrl) {
			this.imageUrl = imageUrl;
		}

		public String getLocalPath() {
			return localPath;
		}

		public void setLocalPath(String localPath) {
			this.localPath = localPath;
		}
		
	}

}
