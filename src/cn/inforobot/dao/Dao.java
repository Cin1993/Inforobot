package cn.inforobot.dao;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.inforobot.database.Data;
import cn.inforobot.pojo.Class_detail;
import cn.inforobot.pojo.Goods_class;
import cn.inforobot.pojo.Host_configure;

public class Dao {
	public List<Host_configure> getHostConfigure(){
		List<Host_configure> host_configure = new ArrayList<Host_configure>();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("DataBase", "info_rob");
		map.put("DataId", "ir0000000001");
		map.put("DataProg", "Dao");
		map.put("DataFunc", "getHostConfigure");
		map.put("DataSelect", "true");
		Data data = new Data();
		try {
			host_configure = data.data(map, Host_configure.class);	
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return host_configure;
	}
	
	public List<Goods_class> getGoodsClass(){
		List<Goods_class> goods_class = new ArrayList<Goods_class>();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("DataBase", "test");
		map.put("DataId", "ir0000000001");
		map.put("DataProg", "Dao");
		map.put("DataFunc", "getGoodsClass");
		map.put("DataSelect", "true");
		Data data = new Data();
		try {
			goods_class = data.data(map, Goods_class.class);	
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return goods_class;
	}
	
	public List<Class_detail> getClassDetail(){
		List<Class_detail> class_details = new ArrayList<Class_detail>();
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("DataBase", "test");
		map.put("DataId", "ir0000000001");
		map.put("DataProg", "Dao");
		map.put("DataFunc", "getClassDetail");
		map.put("DataSelect", "true");
		Data data = new Data();
		try {
			class_details = data.data(map, Class_detail.class);	
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return class_details;
	}
	

}
