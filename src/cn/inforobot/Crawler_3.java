package cn.inforobot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeDriver;
import org.openqa.selenium.firefox.FirefoxDriver;

import cn.inforobot.dao.Dao;
import cn.inforobot.database.DBUtil;
import cn.inforobot.pojo.Host_configure;

public class Crawler_3 {
	private static Dao dao = new Dao();
	public static void main(String[] args) {
		System.setProperty("webdriver.chrome.driver", "C:\\chromedriver.exe");
		List<Host_configure> host_configure = dao.getHostConfigure();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接info_rob数据库
		conn = DBUtil.getConn("info_rob");

		String sql = "select * from class_detail where crawl_depth = '2'";

		// 存放class_details表格3/5列的内容
		Map<String, String> map = new HashMap<String, String>();
		try {
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				map.put(rs.getString(3), rs.getString(5));
			}
			System.out.println(map);
			ArrayList<String> keywordlist = getKeyword();
			//关键词循环
			for (int k = 0; k < keywordlist.size(); k++) {
				//搜索网站的循环
				for (int i = 0; i < host_configure.size(); i++) {
					WebDriver driver = new ChromeDriver();
					//WebDriver driver = new FirefoxDriver();
					String url = host_configure.get(i).getSearch_box_url() + keywordlist.get(k);
					driver.get(url);
					Document doc0 = Jsoup.parse(driver.getPageSource());
					Element nextpage = doc0.select("a[id=pagnNextLink]").first();
					System.out.println(nextpage);
					//翻页
					while (!(nextpage.toString() == null || nextpage.toString().equals(""))) {
						driver.get(url);
						Document doc = Jsoup.parse(driver.getPageSource());
						doc.select("script").remove();
						Elements es = doc.select("ul[id=s-results-list-atf]>li");
						nextpage = doc.select("a[id=pagnNextLink]").first();
						url = nextpage.toString().split("href=\"")[1].split("\"")[0];
						url = "http://www.amazon.com" + url;
						// 存放商品的url
						Iterator<Element> listIterator = es.iterator();
						String temp = "";
						String goods_url = "";
						WebDriver goods_driver = new ChromeDriver();
						//WebDriver goods_driver = new FirefoxDriver();
						while (listIterator.hasNext()) {
							String detail = "";
							Element e = listIterator.next();
							Map<String, String> goods_map = new HashMap<String, String>();
							temp = e.getElementsByAttribute("href").toString();
							if (temp.contains("href=\"")) {
								goods_url = temp.split("href=\"")[1].split("\"")[0];
								if ((goods_url.startsWith("http://") || goods_url.startsWith("https://"))
										&& goods_url.toLowerCase().contains(keywordlist.get(k))) {
									System.out.println(goods_url);
									// 存放商品图片的url
									String img_url = e.select("img[src]").first().toString();
									System.out.println(img_url);
									img_url = img_url.split("src=\"")[1].split("\"")[0];
									
									//延时设置
									//java.util.concurrent.TimeUnit.SECONDS.sleep((int) (Math.random() * 3 + 2));
									
									//跳转到商品页面
									goods_driver.navigate().to(goods_url);
									String goodssrc = goods_driver.getPageSource();
									Document doc_goods = Jsoup.parse(goodssrc);
									
									//goods_map.clear();
									goods_map = getDetail(map, doc_goods, keywordlist.get(k));
									goods_map.put("src", goodssrc);
									//获取商品的状况（新旧商品）
									if (goods_map.get("condition") != null) {
										if (goods_map.get("condition").contains("new"))
											goods_map.put("condition", "used");
										else {
											goods_map.put("condition", "new");
										}
									} else {
										goods_map.put("condition", "new");
									}
										detail = goods_map.get("detail") + goods_map.get("title");
										detail = detail.toUpperCase();
						
										goods_map.put("url", goods_url);
										goods_map.put("img_url", img_url);

										String s1 = "";
										String s2 = "";
										Iterator<Map.Entry<String, String>> entries = goods_map.entrySet().iterator();
										while (entries.hasNext()) {
											Map.Entry<String, String> entry = entries.next();
											if (entries.hasNext()) {
												s1 = s1 + "`" + entry.getKey() + "`" + ", ";
												s2 = s2 + "'" + entry.getValue().replaceAll("'", "''") + "', ";
											} else {
												s1 = s1 + "`" + entry.getKey() + "`";
												s2 = s2 + "'" + entry.getValue().replaceAll("'", "''") + "'";
											}
										}
										s2 = s2.replaceAll("\\\\", "\\\\\\\\");
										try {
											pstmt.execute("insert into goods" + "(" + s1 + ") value(" + s2 + ")");
											System.out.println("插入数据完毕");
											classification();
											System.out.println("分类解析完成");
										} catch (Exception e2) {
											// TODO: handle exception
											e2.printStackTrace();
										}
								}
							}

						}
						goods_driver.quit();
						driver.findElement(By.id("pagnNextString")).click();
					}
					driver.quit();
				}
			}
			DBUtil.closeConn(rs, pstmt, conn);
			classification();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static Map<String, String> getDetail(Map<String, String> map, Document doc_goods, String key_word) {
		
		Map<String, String> info_map = new HashMap<String, String>();
			System.out.println(doc_goods.select("ul[class=a-horizontal a-size-small]>li").text());
			for (Map.Entry<String, String> entry : map.entrySet()) {
				Elements es = doc_goods.select(entry.getValue());
				info_map.put(entry.getKey(), es.text().replaceAll("'", "''"));
			}
			System.out.println(info_map);
		return info_map;
	}

	/*public static Set<String> getclassattribute(String classname) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 杩炴帴test鏁版嵁搴�
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
	}*/

	// 获取搜索关键词
	public static ArrayList<String> getKeyword() {
		ArrayList<String> keywordlist = new ArrayList<String>();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接test数据库
		conn = DBUtil.getConn("info_rob");
		String sql = "select * from keyword where state='Y'";
		try {
			pstmt = conn.prepareStatement("");
			rs = pstmt.executeQuery(sql);
			while (rs.next()) {
				keywordlist.add(rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DBUtil.closeConn(rs, pstmt, conn);
		return keywordlist;
	}

	// 获取搜索结果页面的html代码
	public static String getSourcecode(String searchurl) {
		String sourcecode = "";
		WebDriver driver = new ChromeDriver();
		//WebDriver driver = new FirefoxDriver();
		driver.get(searchurl);
		sourcecode = driver.getPageSource();
		driver.quit();
		System.out.println("获取搜索页代码成功");
		return sourcecode;
	}

	public static Document getGoodsPageCode(String url) {
		Document goods_doc = null;
		WebDriver driver = new ChromeDriver();
		//WebDriver driver = new FirefoxDriver();
		driver.get(url);
		goods_doc = Jsoup.parse(driver.getPageSource());
		driver.quit();
		System.out.println("获取商品详情页代码成功");
		return goods_doc;
	}
	
	public static void classification(){
		Connection conn=DBUtil.getConn("info_rob");
		PreparedStatement ps=null;
		PreparedStatement ps1=null;
		ResultSet rs=null;
		Set<String> set=new HashSet<String>();
		try{
			ps=conn.prepareStatement("");
			ps1=conn.prepareStatement("");
			rs=ps.executeQuery("select distinct class from class_attribute");
			while(rs.next()){
				set.add(rs.getString(1));
			}
			//System.out.println("replace into watch_price (uid,keyword,target_price,requirement) value('"+uid+"','"+keyword+"','"+requirement+"','"+target_price+"')");
			rs=ps.executeQuery("select class_data,url from goods where isnull(class_name)");
			while(rs.next()){
				String s=rs.getString("class_data");
				String url=rs.getString("url");
				System.out.println(url);
				for(String x:set){
					System.out.println(x);
					if(!x.contains("&")){
						System.out.println(s);
						while(s.contains("&")&&s.contains("›")){
							s=s.substring(s.indexOf("›")+1);
							System.out.println(s);
						}
					}
					if(s.contains(x)){
						ps1.execute("update goods set class_name='"+x+"' where url='"+url+"'");
						System.out.println("vvvvvv");
						break;
					}
				}
				
			}
			ps1=null;
			DBUtil.closeConn(rs, ps, conn);
			
		}catch(Exception e){
			e.printStackTrace();
			DBUtil.closeConn(rs, ps, conn);
		}
	}
}
