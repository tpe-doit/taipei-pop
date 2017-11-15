package taipei_pop;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

import org.postgresql.util.PGobject;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.opencsv.CSVReader;

public class buildingLink {

	public static void main(String[] args) throws Exception {
		
		//讀取 csv 的檔案
		String csv_path = "C:\\Users\\felix\\Desktop\\市有閒置空間整合查詢平台\\建物.csv"; 
		CSVReader reader = null;
		HashMap<String, String> keyMap = new HashMap();
		HashMap insertMap = new HashMap();
		int zeroCnt = 0;
		int oneCnt = 0;
		int overCnt = 0;
		int matchCnt = 0;
		try {
			reader = new CSVReader(new FileReader(csv_path));
			String[] nextline;
			int count = 0;
			String targetRowid = "";
			
			while((nextline=reader.readNext()) != null) {
				count++;
				//跳過首行欄位名稱
				if(count == 1)
					continue;
						
				if(nextline != null) {
					
					//System.out.println("地段 : "+nextline[5]);
					//System.out.println("地號母號 : "+nextline[6]);
					//System.out.println("地號子號 : "+nextline[7]);
					//System.out.println("面積 : "+nextline[8]);
					
					keyMap.put("建物標示地段", nextline[3]);
					keyMap.put("建物標示建號母號", nextline[4]);
					keyMap.put("建物標示建號子號", nextline[5]);
					keyMap.put("面積", nextline[10]);
					keyMap.put("門牌", nextline[6]);
					
					//取得目標rowid
					targetRowid = DBSearch(keyMap);
					
					//準備好要 insert 的資訊
					insertMap = parseCSVData(nextline,targetRowid);
					
					//insert
					doInsert(insertMap,targetRowid);
					System.out.println("count : " + count);
				}
			}
					
		}catch(Exception e) {
			System.out.println(e);
		}finally {
			reader.close();
			//System.out.println("zeroCnt : "+zeroCnt);
			//System.out.println("oneCnt : "+oneCnt);
			//System.out.println("overCnt : "+overCnt);
		}
	}
	
	private static void doInsert(HashMap insertMap, String targetRowid) throws Exception {
		//連結資料庫
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/pg_db","PG_USER","PG_PWD");
		String insertSql = "insert into taipei_pop (block, road, road_no, land_no, ser_no01, ser_no02, area, unit, id, geo_json) values "+
				"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " ;
		PreparedStatement pst = con.prepareStatement(insertSql);
		pst.setString(1, insertMap.get("block").toString());
		pst.setString(2, insertMap.get("road").toString());
		pst.setString(3, insertMap.get("road_no").toString());
		pst.setString(4, insertMap.get("land_no").toString());
		pst.setString(5, insertMap.get("ser_no01").toString());
		pst.setString(6, insertMap.get("ser_no02").toString());
		pst.setDouble(7, Double.parseDouble(insertMap.get("area").toString().trim()));
		pst.setString(8, insertMap.get("unit").toString());
		pst.setString(9, insertMap.get("id").toString());
		pst.setObject(10, (PGobject)insertMap.get("geo_json"));
		pst.execute();
		pst.close();
		con.close();
		
	}

	private static HashMap parseCSVData(String[] nextline, String targetRowid) throws Exception {
		String block = nextline[2];
		String road = nextline[3];
		String road_no = nextline[4]+ (nextline[5].equals("")? "": "-"+nextline[5]);
		String land_no = nextline[4]+ (nextline[5].equals("")? "": "-"+nextline[5]);
		String ser_no01 = nextline[3];
		String ser_no02 = nextline[4]+ (nextline[5].equals("")? "": "-"+nextline[5]);
		Double area = Double.parseDouble(nextline[10]);
		String unit = "臺北市政府" + nextline[1];
		String id = "1";
		PGobject geo_json = parseJSONData(nextline, targetRowid);
		
		HashMap dataMap = new HashMap();
		dataMap.put("block", block);
		dataMap.put("road", road);
		dataMap.put("road_no", road_no);
		dataMap.put("land_no", land_no);
		dataMap.put("ser_no01", ser_no01);
		dataMap.put("ser_no02", ser_no02);
		dataMap.put("area", area);
		dataMap.put("unit", unit);
		dataMap.put("id", id);
		dataMap.put("geo_json", geo_json);
		
		return dataMap;
	}

	private static PGobject parseJSONData(String[] nextline, String targetRowid) throws Exception {
		//0. 透過 rowid 找到原始的 geo_json 資料
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/PG_DB","PG_USER","PG_PWD");
		String sql = "select geo_json from taipei_pop where rowid = '"+targetRowid+"'";
		PreparedStatement pst = con.prepareStatement(sql);
		ResultSet Rs = pst.executeQuery();
		String jsonString = "";
		while(Rs.next()) {
			jsonString = Rs.getString("geo_json");
		}
		//1. 得到jsonString 後保留需要的資料，寫入修正後的資料。
		JsonParser parser = new JsonParser();
		JsonObject newJson = new JsonObject();
		JsonObject innerJson = new JsonObject();
		JsonObject oldJson = parser.parse(jsonString).getAsJsonObject();
		JsonObject oldInnerJson = new JsonObject();
		String 建物標示建號 = "";
		PopUtil pop = new PopUtil();
		
		if(oldJson.has("type"))
			newJson.addProperty("type", oldJson.get("type").getAsString());
		if(oldJson.has("properties")) {
			oldInnerJson = oldJson.get("properties").getAsJsonObject();
			//照片檔名
			innerJson.addProperty("Pic_File_Name", "B-"+nextline[0].trim());
			//寫入資料清理後的Data
			//機關別
			innerJson.addProperty("機關別", nextline[1].trim());
			//行政區
			innerJson.addProperty("行政區", nextline[2].trim());
			//建物標示
			建物標示建號 = nextline[4].trim() + (nextline[5].trim().equals("") ? "" : "-"+nextline[5].trim());
			innerJson.addProperty("建物標示", nextline[3].trim()+建物標示建號);
			//門牌 
			innerJson.addProperty("門牌", "臺北市"+ nextline[2].trim()+ nextline[6].trim());
			//樓層/總樓層
			innerJson.addProperty("樓層(總樓層)", nextline[8].trim()+"("+nextline[9].trim()+")");
			//建築完成日期
			innerJson.addProperty("建築完成日期", nextline[7].trim());
			//閒置面積
			innerJson.addProperty("閒置面積(m2)", nextline[10].trim());
			//取得價格
			innerJson.addProperty("取得價格", "新台幣 "+pop.addPriceComma(nextline[11].trim())+ " 元");
			//閒置原因
			innerJson.addProperty("閒置原因", nextline[12].trim());
			//房屋現況
			innerJson.addProperty("房屋現況", nextline[13].trim());
			//列管日期
			innerJson.addProperty("列管日期", nextline[14].trim().replace(".", "-"));
			//原使用用途
			innerJson.addProperty("原使用用途", nextline[15].trim());
			//土地所有權人
			innerJson.addProperty("土地所有權人", nextline[17].trim());
			//土地管理機關
			innerJson.addProperty("土地管理機關", nextline[18].trim());
			//使用分區
			innerJson.addProperty("使用分區", nextline[19].trim());
			//計畫內容
			innerJson.addProperty("計畫內容", nextline[20].trim());
			//目前執行情形
			innerJson.addProperty("目前執行情形", nextline[21].trim());
			//加完後寫入 newJson
			newJson.add("properties", innerJson);
		}
		if(oldJson.has("geometry"))
			newJson.add("geometry", oldJson.get("geometry").getAsJsonObject());
		
		
		//PGobject
		PGobject jsonObject = new PGobject();
		jsonObject.setType("json");
		jsonObject.setValue(pop.stringToUnicode(newJson.toString()));
				
		return jsonObject;
	}

	private static String DBSearch(HashMap<String, String> keyMap) throws Exception {
		//System.out.println("********* Into DBSearch *********");
		
		//連線psql 下 sql 指令
		String sql = "";
		Class.forName("org.postgresql.Driver");
		Connection con = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/PG_DB","PG_USER","PG_PWD");
		PreparedStatement stmt = con.prepareStatement("select geo_json, rowid from taipei_pop;");
		ResultSet Rs = stmt.executeQuery();
		//gson
		JsonParser parser = new JsonParser();
		
		//init data
		String jsonString = "";
		String 建物標示 = "";
		String[] 建物標示Cut = {"", ""};
		String 面積 = "";
		String 建號String = "";
		String 小段String = "";
		PopUtil pop = new PopUtil();
		boolean check地段 = false;
		boolean check地號 = false;
		boolean check面積 = false;
		int popIndex = 0;
		int matchCnt = 0;
		String rowid = "";
		//逐筆檢查
		while (Rs.next()) {
			popIndex ++;
			//System.out.println("Index : "+popIndex);
			jsonString = Rs.getString("geo_json");
			
			if(isLand(jsonString)) {
				//取得每筆在 taipei_pop 的資料
				if(parser.parse(jsonString).getAsJsonObject().get("properties").getAsJsonObject().has("建物標示")) {
					建物標示 = parser.parse(jsonString)
							.getAsJsonObject().get("properties")
							.getAsJsonObject().get("建物標示")
							.getAsString();
				}
				
				if(parser.parse(jsonString).getAsJsonObject().get("properties").getAsJsonObject().has("閒置面積㎡")) {
					面積 = parser.parse(jsonString)
							.getAsJsonObject().get("properties")
							.getAsJsonObject().get("閒置面積㎡")
							.getAsString();
				}
				
				//建物標示 prase
				if(建物標示.contains("小段")) {
					建物標示Cut[1] = 建物標示.split("小段")[1];
					建物標示Cut[0] = 建物標示.split("小段")[0].split("段")[0];
					小段String = 建物標示.split("小段")[0].split("段")[1];
					建物標示 = 建物標示Cut[0] + "段" + pop.arabicToChinese(小段String) + "小段" +建物標示Cut[1];					
				}
				
				//資料筆對
				check地段 = 建物標示.contains(keyMap.get("建物標示地段"));
				
				建號String = keyMap.get("建物標示建號母號");
				if(!keyMap.get("建物標示建號子號").replace(" ", "").equals("")) 
					建號String += "-"+keyMap.get("建物標示建號子號");
				
				//System.out.println(建號String);
				
				check地號 = 建物標示.contains(建號String);
				check面積 = 面積.contains(keyMap.get("面積"));
				
				//System.out.println(建物標示+"     "+keyMap.get("建物標示地段")+"     "+keyMap.get("建物標示地段")+"     "+keyMap.get("建物標示建號子號"));
				//System.out.println(面積+"     "+keyMap.get("面積"));
				
				if(check地段 && check地號 && check面積) {
					//System.out.println("找到match : "+popIndex);
					matchCnt++;
					rowid = Rs.getString("rowid");
				}
			}
			
		}
		
		stmt.close();
		con.close();
		
		//手動 mapping 
		if(matchCnt == 0) {
			if(keyMap.get("建物標示地段").equals("力行段三小段")) {
				rowid = "118";
				matchCnt = 1;
			}else if(keyMap.get("建物標示地段").equals("華岡段二小段")) {
				rowid = "119";
				matchCnt = 1;
			}else if(keyMap.get("建物標示地段").equals("萬隆段二小段")) {
				rowid = "125";
				matchCnt = 1;
			}
		}
		
		if(matchCnt > 1) {
			if(keyMap.get("面積").equals("58.94") && keyMap.get("門牌").contains("199")) {
				rowid = "93";
				matchCnt = 1;
			}else if(keyMap.get("面積").equals("58.94") && keyMap.get("門牌").contains("197")) {
				rowid = "94";
				matchCnt = 1;
			}else if(keyMap.get("面積").equals("57.55") && keyMap.get("門牌").contains("1樓")) {
				rowid = "101";
				matchCnt = 1;
			}else if(keyMap.get("面積").equals("57.55") && keyMap.get("門牌").contains("2樓")) {
				rowid = "102";
				matchCnt = 1;
			}else if(keyMap.get("面積").equals("56.87") && keyMap.get("門牌").contains("1樓")) {
				rowid = "103";
				matchCnt = 1;
			}else if(keyMap.get("面積").equals("56.87") && keyMap.get("門牌").contains("2樓")) {
				rowid = "104";
				matchCnt = 1;
			}else if(keyMap.get("面積").equals("71.59") && keyMap.get("門牌").contains("1樓")) {
				rowid = "105";
				matchCnt = 1;
			}else if(keyMap.get("面積").equals("71.59") && keyMap.get("門牌").contains("2樓")) {
				rowid = "106";
				matchCnt = 1;
			}
		}
		//System.out.println("rowid : "+rowid);
		
		return rowid;
	}
	
	private static boolean areaCheck(String 面積0, String 面積1) {
		面積0 = 面積0.replace(",", "");
		面積1 = 面積1.replace(",", "");
		//System.out.println(面積0+"     "+面積1);
		double value0 = Double.parseDouble(面積0);
		double value1 = Double.parseDouble(面積1);
		return value0 == value1;
	}

	private static boolean isLand(String jsonString) {
		JsonParser parser = new JsonParser();
		String geo_type = parser.parse(jsonString)
				.getAsJsonObject().get("geometry")
				.getAsJsonObject().get("type")
				.getAsString().toLowerCase();
		boolean check0 = geo_type.equals("point");
		boolean check1 = parser.parse(jsonString)
				.getAsJsonObject().get("properties")
				.getAsJsonObject().has("建物標示");
		boolean check2 = geo_type.equals("polygon") || geo_type.equals("multipolygon");
		boolean check3 = parser.parse(jsonString)
				.getAsJsonObject().get("properties")
				.getAsJsonObject().has("105公告地價");
		if(check0 && check1) {
			return true;
		}else {
			return false;
		}
	}

}
