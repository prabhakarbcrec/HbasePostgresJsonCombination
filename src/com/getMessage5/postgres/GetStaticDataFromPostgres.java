package com.getMessage5.postgres;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.combination.checkMessage.AppendMessageHere;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class GetStaticDataFromPostgres {
	static long I;
	static String C;
	static String S;
	static int l;
	static int b;
	
	PreparedStatement pre=null;
	public  String getStaticDataOntheBasisOfMmsiNumber(String mmsi,	Connection con,JsonObject json) throws SQLException {
		///System.out.println("this is mmsi whic came as an argument: - "+ mmsi+"  "+json);
		try {
			PreparedStatement pre = con.prepareStatement("select * from tablename where no="+"\'"+mmsi+"\'");
			ResultSet r = pre.executeQuery();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			AppendMessageHere jsonappends=new AppendMessageHere();
			I=0;
			C=null;
			Name=null;
			length=0;
			breadth=0;
			while (r.next()) {
				I = r.getLong("I_no");
				C = r.getString("c");
				Name = r.getString("name");
				length = r.getInt("length");
				breadth = r.getInt("breadth");
				//System.out.println("this is all info: "+I + "  " + C+"  "+Name+"  "+length+"  "+breadth);
				
			}
			jsonappends.appendMessage(I,C,Name,breadth,length,json);
			pre.close();
		} catch (Exception e) {
			
			//e.printStackTrace();
			
		}
		return "this is last";

	}
	

}
