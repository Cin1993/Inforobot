package cn.inforobot;

import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import cn.inforobot.dao.Dao;
import cn.inforobot.database.DBUtil;
import cn.inforobot.download.DownLoader;
import cn.inforobot.pojo.Host_configure;

public class Crawler_2 {
	private static Dao dao = new Dao();
	private static DownLoader downloader = new DownLoader();

	public static void main(String[] args) {
		List<Host_configure> host_configure = dao.getHostConfigure();


		try {
			String key_word = "iphone";
			for (int i = 0; i < host_configure.size(); i++) {

				String url = host_configure.get(i).getSearch_box_url() + key_word;
				System.out.println(url);
				Document doc0 = Jsoup.parse(downloader.getURLSource(new URL(url)));
				Element nextpage = doc0.select("a[id=pagnNextLink]").first();
				while (!(nextpage.toString() == null || nextpage.toString().equals(""))) {
					java.util.concurrent.TimeUnit.SECONDS.sleep(3);
					Document doc = Jsoup.parse(downloader.getURLSource(new URL(url)));
					// Document doc = Jsoup.connect(url).get();
					Elements es = doc.select("ul[id=s-results-list-atf]>li");
					nextpage = doc.select("a[id=pagnNextLink]").first();
					System.out.println(nextpage);
					url = nextpage.toString().split("href=\"")[1].split("\"")[0];
					url = "http://www.amazon.com"+url;
					System.out.println(url);
					// 存放商品url
					Iterator<Element> listIterator = es.iterator();
					while(listIterator.hasNext()){
						System.out.println(listIterator.next().text());
					}
				}
			}
		}catch (Exception e) {
				// TODO: handle exception
			e.printStackTrace();
			}
					

	}

	public static Map<String, String> getDetail(Map<String, String> map, Document doc_goods, String key_word) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接test数据库
		conn = DBUtil.getConn("test");
		Map<String, String> info_map = new HashMap<String, String>();
		try {
			String class_check = "";
			pstmt = conn.prepareStatement("");
			rs = pstmt.executeQuery("select * from class where key_word = '" + key_word + "'");
			while (rs.next()) {
				class_check = rs.getString(3);
			}
			System.out.println(doc_goods.select("ul[class=a-horizontal a-size-small]>li").text());
			if (!doc_goods.select("ul[class=a-horizontal a-size-small]>li").text().toLowerCase().replaceAll(" ", "")
					.endsWith(class_check)) {
				info_map.put("check", "false");
			}
			for (Map.Entry<String, String> entry : map.entrySet()) {
				Elements es = doc_goods.select(entry.getValue());
				info_map.put(entry.getKey(), es.text().replaceAll("'", "''"));
			}
			System.out.println(info_map);
			DBUtil.closeConn(rs, pstmt, conn);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return info_map;

	}

	public static Set<String> getclassattribute(String classname) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接test数据库
		conn = DBUtil.getConn("test");
		Set<String> classattribute = new HashSet<String>();
		try {
			pstmt = conn.prepareStatement("");
			rs = pstmt.executeQuery("select * from class_attribute where class = '" + classname + "'");
			while (rs.next()) {
				classattribute.add(rs.getString(2));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DBUtil.closeConn(rs, pstmt, conn);
		return classattribute;
	}

}
