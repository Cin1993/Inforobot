package cn.inforobot;

import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cn.inforobot.dao.Dao;
import cn.inforobot.database.DBUtil;
import cn.inforobot.download.DownLoader;
import cn.inforobot.pojo.Host_configure;

public class Crawler {
	private static Dao dao = new Dao();
	private static DownLoader downloader = new DownLoader();

	public static void main(String[] args) {
		List<Host_configure> host_configure = dao.getHostConfigure();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接test数据库
		conn = DBUtil.getConn("test");

		String sql = "select * from class_detail where class_name = 'cellphone' or class_name = 'others' and crawl_depth = '2'" ;

		// map存放class_detail表格3/5列的内容
		Map<String, String> map = new HashMap<String, String>();
		try {
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				map.put(rs.getString(3), rs.getString(5));
			}

			String key_word = "iphone";
			for (int i = 0; i < host_configure.size(); i++) {
				String url = host_configure.get(i).getSearch_box_url() + key_word;// +"&page=2"

				Document doc = Jsoup.parse(downloader.getURLSource(new URL(url)));
				// Document doc = Jsoup.connect(url).get();
				Elements e = doc.select("ul[id=s-results-list-atf]>li");

				// 存放商品url
				ListIterator<Element> listIterator = e.listIterator();
				String temp = "";
				String goods_url = "";
				while (listIterator.hasNext()) {
					Map<String, String> goods_map = new HashMap<String, String>();
					java.util.concurrent.TimeUnit.SECONDS.sleep(3);
					temp = listIterator.next().getElementsByAttribute("href").toString();
					if (temp.contains("href=\"")) {
						goods_url = temp.split("href=\"")[1].split("\"")[0];
						if (goods_url.startsWith("http://")) {
							System.out.println(goods_url);
							Document doc_goods = Jsoup.parse(downloader.getURLSource(new URL(goods_url)));
							//goods_map = getBookDetail(map, doc_goods);
							goods_map = getCellphoneDetail(map, doc_goods);
							goods_map.put("url", goods_url);
							
							String s1 = "";
							String s2 = "";
							Iterator<Map.Entry<String, String>> entries = goods_map.entrySet().iterator();
							while (entries.hasNext()) {
								Map.Entry<String, String> entry = entries.next();
								if (entries.hasNext()) {
									s1 = s1 + "`" + entry.getKey() + "`" + ", ";
									s2 = s2 + "'" + entry.getValue() + "', ";
								} else {
									s1 = s1 + "`" + entry.getKey() + "`";
									s2 = s2 + "'" + entry.getValue() + "'";
								}
							}
							System.out.println("insert into cellphone(" + s1 + ") value(" + s2 + ")");
							pstmt.execute("insert into cellphone(" + s1 + ") value(" + s2 + ")");
						}
					}
				}
			}
			DBUtil.closeConn(rs, pstmt, conn);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static Map<String, String> getBookDetail(Map<String, String> map, Document doc_goods) {
		Map<String, String> info_map = new HashMap<String, String>();
		String s = "";
		for (Map.Entry<String, String> entry : map.entrySet()) {
			Elements es = doc_goods.select(entry.getValue());
			for (Element e : es) {
				if (entry.getKey().equals("author")) {
						
						s = e.text();
						s = s.replaceAll("'", "''");
						info_map.put(entry.getKey(), s);
					
				}
				if (entry.getKey().equals("price")){
					s = e.text();
					info_map.put(entry.getKey(), s);
				}
				if (e.text().toLowerCase().contains(entry.getKey())) {
					s = e.html().split(" ")[1].split("</li>")[0];
					s = s.replaceAll("'", "''");
					info_map.put(entry.getKey(), s);
				}
			}
			System.out.println(entry.getValue());
		}
		return info_map;
	}

	public static Map<String, String> getCellphoneDetail(Map<String, String> map, Document doc_goods) {
		System.out.println(map);
		Map<String, String> info_map = new HashMap<String, String>();
		String s = "";
		for (Map.Entry<String, String> entry : map.entrySet()) {
			Elements es = doc_goods.select(entry.getValue());
			for (Element e : es) {
				if (entry.getKey().equals("color")) {
					if (e.text().contains("Color")) {
						s = e.text().split("Color:")[1];
						System.out.println(s);
						if(s.contains("|")){
							s = s.split("\\|")[0];
						}
						System.out.println(s);
						info_map.put(entry.getKey(), s);
						System.out.println(info_map);
					}
				}
				if (entry.getKey().equals("size")) {
					if (e.text().contains("Size")) {
						s = e.text().split("Size:")[1];
						System.out.println(s);
						if(s.contains("|")){
							s = s.split("\\|")[0];
						}
						System.out.println(s);
						info_map.put(entry.getKey(), s);
						System.out.println(info_map);
					}
				}
				if (entry.getKey().equals("model")) {
					if (e.text().contains("Item model number")) {
						s = e.text();
						info_map.put(entry.getKey(), s);
					}
				}
				if (entry.getKey().equals("factory")) {
					s = e.text();
					info_map.put(entry.getKey(), s);
				}
				if (entry.getKey().equals("title")){
					s = e.text();
					info_map.put(entry.getKey(), s);
				}
				if (entry.getKey().equals("price")){
					s = e.text();
					info_map.put(entry.getKey(), s);
				}
				
			}
		}
		return info_map;
	}
	
	
	
	
}
