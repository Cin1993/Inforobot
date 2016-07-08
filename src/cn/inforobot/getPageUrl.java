package cn.inforobot;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cn.inforobot.dao.Dao;
import cn.inforobot.download.DownLoader;
import cn.inforobot.pojo.Host_configure;

public class getPageUrl {
	private static Dao dao = new Dao();
	private static DownLoader downloader = new DownLoader();
	public static void main(String[] args) {
		List<Host_configure> host_configure = dao.getHostConfigure();
		String key_word = "iphone";
		int pagenum = 1;
		for(int i = 0;i < host_configure.size();i++){
			String url = host_configure.get(i).getSearch_box_url() + key_word +"&page="+pagenum;
			Document doc;
			try {
				url = "http://www.amazon.com/s/ref=nb_sb_noss?url=search-alias%3Daps&field-keywords=iphone";
				doc = Jsoup.parse(downloader.getURLSource(new URL(url)));
				String s = doc.select("h2[id=s-result-count]").text();
				//s = s.split(" ")[0].split("-")[1];
				s = s.split(" results")[0].split("of ")[1];
				System.out.println(s);
				int goods_amount = Integer.valueOf(s.replaceAll(",", ""));
				System.out.println(goods_amount);
				
				s = doc.select("h2[id=s-result-count]").text();
				s = s.split(" of")[0].split("-")[1];
				int page = Integer.valueOf(s.replaceAll(",", ""));
				System.out.println(s);
				//System.out.println(s);
				
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			
			
		}
	}
	
	

}
