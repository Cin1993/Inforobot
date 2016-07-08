package cn.inforobot.database;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Program Purpose: Get Site and Search Program
// Program input: N/A
// Program output: N/A
// Program data/time: 2015/7/1 13:32
// Create by: Jack
// Create time: 2015/7/1
// Program start
public class Data {
	// Program Purpose: Get Search Function
	// Program input: boolean, String, String,String, List<SqlBean>
	// Program output:List
	// Program data/time: 2015/7/1 13:32
	// Create by: Jack
	// Create time: 2015/7/1
	// Program start
	@SuppressWarnings("unchecked")
	public <T> List<T> data(Map<String, Object> map,Class<?> cla) throws SQLException, InstantiationException, IllegalAccessException {
		
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		ResultSetMetaData m = null;
		
		//连接extractor库，该库存放需要执行的SQL语句
		conn = DBUtil.getConn("info_rob");
		ArrayList<Object> ret = new ArrayList<Object>();
		
		String sql = "select ir_sql_clause from ir_sql_tbl "
				+ "where ir_id = '" + map.get("DataId") + "' and ir_prog_name='" + map.get("DataProg")
				+ "' and ir_func_name='" + map.get("DataFunc") + "' and ir_func_seq=1";
		pstmt = conn.prepareStatement(sql);
		rs = pstmt.executeQuery();
		while (rs.next()) {
			//获取具体要操作的sql语句
			sql = rs.getString("ir_sql_clause");
		}
		//关闭数据库连接
		DBUtil.closeConn(rs, pstmt, conn);

		Field[] field = cla.getDeclaredFields();  //获取所有成员变量存放到一个数组中
		conn = DBUtil.getConn(map.get("DataBase").toString());
		
		//正则表达式匹配该类型{#  }的字符串
		String regex = "\\{\\#(.*?)\\}";
		Pattern pattern = Pattern.compile(regex);
	    Matcher matcher = pattern.matcher(sql);
	    while (matcher.find()) {
	           //System.out.println(matcher.group(1));
	           map.get(matcher.group(1));
	           //将匹配到的内容替换到sql语句中
	           sql = sql.replace("{#"+matcher.group(1)+"}", "'"+map.get(matcher.group(1))+"'");
	           
	    }
		//System.out.println("sql:"+sql);
		pstmt = conn.prepareStatement(sql);
		if (map.get("DataSelect").equals("true")) {
			rs = pstmt.executeQuery();
			m = rs.getMetaData();
			int columns = m.getColumnCount();
			if (columns > 0) {
				while (rs.next()) {
					//创建类的实例，即创建传入的类的对象
					Object bean = cla.newInstance();
					//循环判断类里面成员的数据类型
					for(int i=0;i<field.length;i++){
						//设置类的成员变量可以访问
						field[i].setAccessible(true);
						//获取类的成员变量的类型
						//System.out.println(field[i].getType().getSimpleName());
						//根据成员变量的类型进行判断并进行处理
						switch(field[i].getType().getSimpleName()){
						//如果为Long类型，将bean的值设置为数据库中的值
						case "Long":field[i].set(bean,rs.getLong(field[i].getName()));break;
						case "Timestamp":
							field[i].set(bean,rs.getTimestamp(field[i].getName()));break;
						case "Array":field[i].set(bean,rs.getArray(field[i].getName()));break;
						case "AsciiStream":field[i].set(bean,rs.getAsciiStream(field[i].getName()));break;
						case "BigDecimal":field[i].set(bean,rs.getBigDecimal(field[i].getName()));break;
						case "int":field[i].set(bean,rs.getInt(field[i].getName()));break;
						default:field[i].set(bean,rs.getString(field[i].getName()));
							break;
						}
					}
					ret.add(bean);
				}
			}
		} else {
			//关闭数据库连接
			DBUtil.closeConn(rs, pstmt, conn);
			return null;
		}
		//关闭数据库连接
		DBUtil.closeConn(rs, pstmt, conn);
		//返回的List存放数据库中的值
		return (List<T>) ret;
	}
	// Program end
	private Object whereValue = "";//where条件字段值
	private String operaType ="";//数据操作类型
	private String whereField ="";//where 条件字段
	private String insertStartSql;
	private String insertEndSql;
	private String updateSql;
	private String delSql;
	@SuppressWarnings("static-access")
	//更新表格数据的方法
	public List<String> getUpdateTblData(Map<String, Object> map){
		//建立数据库连接
		DBUtil dbUtil = new DBUtil();
		ResultSet rs = null ;
		PreparedStatement stmt = null;
		ResultSetMetaData data;
		Connection conn = null;
		conn = dbUtil.getConn("data_definition");
		List<String> ret = new ArrayList<String>();
		//sql语句获取30min内更新的数据
		String sql="select * from "+ map.get("tblName")+" where update_dttm <= now() and  update_dttm>= date_sub(now(), interval 30 minute)";
		insertStartSql="insert into "+ map.get("tblName")+" (";
		insertEndSql="values(";
		updateSql="update "+ map.get("tblName")+" set ";
		delSql="delete  from "+ map.get("tblName")+" where ";
		String updateActionSql ="";
		try {
			List<String> sqlList = null;
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery(sql);
			data = rs.getMetaData();
				while (rs.next()) {
					for (int i = 1; i <= data.getColumnCount(); i++) {
						    // 获得指定列的列名
							String columnName = data.getColumnName(i);
							// 获得指定列的类型
							String columnTypeName = data.getColumnTypeName(i);
							whereField =data.getColumnName(1);
							String descrVal = rs.getString(whereField);
							Object valStr = null;
							switch (columnTypeName){
							case "LONG":
								valStr = rs.getLong(columnName);//获取列的值
								sqlList=null;
								sqlList = sqlStrList(rs,valStr,columnName);
								break;
							case "TIMESTAMP":
								SimpleDateFormat format = new SimpleDateFormat("yyyy-M-dd HH:mm:ss");
								valStr = rs.getTimestamp(columnName);
								String ctime = format.format(valStr);
								sqlList=null;
								sqlList = sqlStrList(rs,ctime,columnName);
								break;
							case "ARRAY":
								valStr = rs.getArray(columnName);
								sqlList=null;
								sqlList = sqlStrList(rs,valStr,columnName);
							    break;
							case "ASCIISTREAM":
								valStr = rs.getAsciiStream(columnName);
								sqlList=null;
								sqlList = sqlStrList(rs,valStr,columnName);
								break;
							case "BIGDECIMAL":
								valStr = rs.getBigDecimal(columnName);
								sqlList=null;
								sqlList = sqlStrList(rs,valStr,columnName);
								break;
							case "INT":
								valStr = rs.getInt(columnName);
								sqlList=null;
								sqlList = sqlStrList(rs,valStr,columnName);
							    break;
							default:
								valStr = rs.getString(columnName);
							     sqlList=null;
							    sqlList = sqlStrList(rs,valStr,columnName);
							    break;
							}
							
							if("update_action".equals(columnName) && !"P".equals(valStr)){
								updateActionSql = "update "+ map.get("tblName")+" set update_action='P' where disease_short_descr="+"'"+descrVal+"'";
							}
						}
						if("I".equals(operaType)&& sqlList!=null){
							String sqlstr="";
							for(int i1=0;i1<sqlList.size();i1++){
								String sql1="";
								sql1 = sqlList.get(i1).substring(0,sqlList.get(i1).lastIndexOf(","));
								sql1+=")";
								sqlstr+=sql1;
							}
							//System.out.println(sqlstr);
							ret.add(sqlstr);
							sqlstr="";
							insertStartSql="";
							insertEndSql="";
							insertStartSql="insert into "+ map.get("tblName")+" (";
							insertEndSql="values(";
						}else if("U".equals(operaType) && sqlList!=null && sqlList.size()>0){
							String sqlstr="";
							for(int i1=0;i1<sqlList.size();i1++){
								String sql1="";
								sql1 = sqlList.get(i1).substring(0,sqlList.get(i1).lastIndexOf(","));
								sql1+=" where "+whereField+" ="+"'"+whereValue+"'";
								sqlstr+=sql1;
							}
							//System.out.println(sqlstr);
							ret.add(sqlstr);
							sqlstr="";
							updateSql="";
							updateSql="update "+ map.get("tblName")+" set ";
						}else if("D".equals(operaType)){
							delSql+=whereField+" = "+"'"+whereValue+"'";
							ret.add(delSql);
							delSql="";
							delSql="delete  from "+ map.get("tblName")+" where ";
						} 
						//更新update_action 为P
						if(!"".equals(updateActionSql)){
							stmt = conn.prepareStatement(updateActionSql);
							stmt.execute();
						}
					}
		 } catch (Exception e) {
			e.printStackTrace();
		}finally{
			dbUtil.closeConn(rs,stmt,conn);
		}	
		
		return (List<String>) ret;
	}
	
	public List<String> sqlStrList(ResultSet rs,Object valStr,String filedName) throws SQLException{
		List<String> sqlList = new ArrayList<>();
		if(rs.getString("update_action").equals("I")){
			operaType = "I";
			insertStartSql+=filedName+",";
			if(valStr==null){
				insertEndSql+=valStr+",";
			}else{
				insertEndSql+="'"+valStr+"'"+",";
				
			}
			sqlList.add(insertStartSql);
			sqlList.add(insertEndSql);
		}else if(rs.getString("update_action").equals("U")){
			operaType = "U";
			if(!"".equals(valStr) && valStr!=null ){
				updateSql+=filedName+"=";
				updateSql+="'"+valStr+"'"+",";
			}
			if(whereField.equals(filedName)){//第一个字段为条件字段
				whereValue = valStr;
			}
			sqlList.add(updateSql);
		}else if(rs.getString("update_action").equals("D")){
			operaType = "D";
			if(whereField.equals(filedName)){//第一个字段为条件字段
				whereValue = valStr;
			}
		}
		
		return sqlList;
	}

	
	@SuppressWarnings("unchecked")
	public <T> List<T> dataDefinition(Map<String, Object> map,Class<?> cla) throws SQLException, InstantiationException, IllegalAccessException {
		//数据库连接
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		ResultSetMetaData m = null;
		conn = DBUtil.getConn("data_definition");
		ArrayList<Object> ret = new ArrayList<Object>();

		String sql = (String) map.get("sql");
		
		//获取class的成员变量
		Field[] field = cla.getDeclaredFields();
		//System.out.println("sql:"+sql);
		pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			m = rs.getMetaData();
			int columns = m.getColumnCount();
			if (columns > 0) {
				while (rs.next()) {
					Object bean = cla.newInstance();
					for(int i=0;i<field.length;i++){
						field[i].setAccessible(true);
						System.out.println(field[i].getType().getSimpleName());
						switch(field[i].getType().getSimpleName()){
						case "Long":field[i].set(bean,rs.getLong(field[i].getName()));break;
						case "Timestamp":
							field[i].set(bean,rs.getTimestamp(field[i].getName()));break;
						case "Array":field[i].set(bean,rs.getArray(field[i].getName()));break;
						case "AsciiStream":field[i].set(bean,rs.getAsciiStream(field[i].getName()));break;
						case "BigDecimal":field[i].set(bean,rs.getBigDecimal(field[i].getName()));break;
						case "int":field[i].set(bean,rs.getInt(field[i].getName()));break;
						default:field[i].set(bean,rs.getString(field[i].getName()));
							break;
						}
					}
					ret.add(bean);
				}
			}
		DBUtil.closeConn(rs, pstmt, conn);
		return (List<T>) ret;
	}
}
// Program end
