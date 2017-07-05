package banana.dowloader.processor;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import banana.core.download.impl.DefaultHttpDownloader;
import banana.core.request.PageRequest;
import banana.core.request.PageRequest.PageEncoding;
import banana.core.request.RequestBuilder;


public class ArticleContent {
	
	private String url;
	
	private String baseUrl;
	
	private String html;
	
	private String article_tag;
	
	private StringBuilder article = new StringBuilder();
	
	private String lastAppend ;
	
	public ArticleContent(String url,String html, String article_tag) {
		this.url = url;
		this.baseUrl = url.substring(0, url.indexOf("/", 7));
		this.html = html;
		this.article_tag = article_tag;
		Document doc = Jsoup.parse(html);
		Element element = doc.select(article_tag).first();
		if (element == null){
			System.err.println(article_tag + " 未找到");
			return;
		}
		//System.out.println(element.html());
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
		if (url.startsWith("http")){
			this.article.append(url);
		}else if (url.startsWith("/")){
			this.article.append(baseUrl+url);
		}
	}

	public String getArticle() {
		String cont = article.toString();
		while(cont.startsWith("\n")){
			cont = cont.substring(1, cont.length());
		}
		return cont;
	}

//	public static void main(String[] args) {
//		DefaultHttpDownloader downloader = new DefaultHttpDownloader();
//		PageRequest request = (PageRequest) new RequestBuilder().setUrl("http://www.51feibao.com/article-view-4498.html").setPageEncoding(PageEncoding.GB2312).build();
//		String html = downloader.download(request).getContent();
//		ArticleContent articleContent = new ArticleContent("http://www.fsdpp.cn/yis", html, "#content div.col_main");
//		System.out.println(articleContent.getArticle());
//	}

}
