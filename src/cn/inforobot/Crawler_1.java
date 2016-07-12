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

public class Crawler_1 {
	private static Dao dao = new Dao();
	private static DownLoader downloader = new DownLoader();

	public static void main(String[] args) {
		List<Host_configure> host_configure = dao.getHostConfigure();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接test数据库
		conn = DBUtil.getConn("test");

		String sql = "select * from class_detail where class_name = 'others' and crawl_depth = '2'";

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
					url = nextpage.toString().split("href=\"")[1].split("\"")[0];
					url = "http://www.amazon.com"+url;
					System.out.println(nextpage.toString());

					// 存放商品url
					Iterator<Element> listIterator = es.iterator();
					String temp = "";
					String goods_url = "";
					while (listIterator.hasNext()) {

						String detail = "";
						Set<String> classattributehs = new HashSet<String>();
						String classname = "";
						Element e = listIterator.next();
						Map<String, String> goods_map = new HashMap<String, String>();
						java.util.concurrent.TimeUnit.SECONDS.sleep(3);
						temp = e.getElementsByAttribute("href").toString();
						if (temp.contains("href=\"")) {
							goods_url = temp.split("href=\"")[1].split("\"")[0];
							if (goods_url.startsWith("http://") && goods_url.toLowerCase().contains(key_word)) {
								// goods_url.startsWith("https://") ||
								System.out.println(goods_url);
								// 抓取图片url
								String img_url = e.select("img[src]").first().toString();
								img_url = img_url.split("<img src=\"")[1].split("\"")[0];

								System.out.println(img_url);
								Document doc_goods = Jsoup.parse(downloader.getURLSource(new URL(goods_url+"/ref=sr_1_2?ie=UTF8&qid=1465193314&sr=8-2&keywords=iphone")));
								// Document doc_goods =
								// Jsoup.connect(goods_url).get();
								// goods_map = getBookDetail(map, doc_goods);
								goods_map.clear();
								goods_map = getDetail(map, doc_goods, key_word);

								// 判断新旧
								if (goods_map.get("condition") != null) {
									if (goods_map.get("condition").contains("new"))
										goods_map.put("condition", "used");
									else {
										goods_map.put("condition", "new");
									}
								} else {
									goods_map.put("condition", "new");
								}

								if (goods_map.get("check") == null) {

									detail = goods_map.get("detail") + goods_map.get("title");
									detail = detail.toUpperCase();
									System.out.println(detail);
									rs = pstmt.executeQuery("select * from class where key_word = '" + key_word + "'");
									if (rs.next()) {
										classname = rs.getString(2);
									} else {
										classname = "others";
									}
									classattributehs = getclassattribute(classname);

									for (String s : classattributehs) {
										rs = pstmt.executeQuery(
												"select * from attribute_parameter where attribute = '" + s + "'");
										while (rs.next()) {
											String parameter = rs.getString(2);
											if (detail.contains(parameter)) {
												goods_map.put(s, parameter);
											}

										}
									}
									goods_map.put("url", goods_url);
									goods_map.put("img_url", img_url);

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
									System.out.println("insert into " + classname + "(" + s1 + ") value(" + s2 + ")");
									pstmt.execute("insert into " + classname + "(" + s1 + ") value(" + s2 + ")");
								}
							}
						}
					}
				}
			}
			DBUtil.closeConn(rs, pstmt, conn);
		} catch (Exception e) {
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
