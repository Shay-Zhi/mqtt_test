package Utils;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import mqtt_test.DBConn;

public class myUtils {
	private String host = "tcp://218.17.87.115:1883";		// MQTT服务端
    private String id;									// MQTT客户端的ID
    private String myTopic = "5122";					// 默认的订阅主题
    private MqttConnectOptions options;
    
    private DBConn conn = new DBConn();
    
    public String mqttTopic() {
    	return this.myTopic;
    }
    
    public void setMqttTopic(String myTopic) {
    	this.myTopic = myTopic;
    }
    
    /**
     * 创建客户端
     * @param myTopic
     * @param qos
     */
    public MqttClient createClient(String myTopic, int qos) {
    	try {
    		MqttCallback callback = null;		
    		MqttClient client = new MqttClient(host, id, new MemoryPersistence());		
            options = new MqttConnectOptions();			
            options.setConnectionTimeout(10);		
            options.setKeepAliveInterval(20000);		
            options.setAutomaticReconnect(true);	
            options.setCleanSession(false);	
            client.setCallback(new MqttCallbackExtended() {		
            	@Override
                public void connectionLost(Throwable arg0) {
                	Date date = new Date();
			        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");
                    System.out.println(df.format(date) + id + " 连接异常断开  " + arg0);
                    if(conn != null) {
                        //conn.closeDB();
                    }
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken arg0) {
                    System.out.println(id + " deliveryComplete " + arg0);
                    if(conn != null) {
                        //conn.closeDB();
                    }
                }

                @Override
                public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
                	System.out.println(id + " messageArrived: " + arg1.toString());
                    
                    Map<String,String> payload = new HashMap();
                    System.out.println("开始解析数据");
                    payload = parsePayload(arg1);
                    if(payload == null) {
                    	System.out.println("数据有误，停止数据保存操作 ...");
                    	return;
                    }
                    System.out.println("开始保存数据");
                    save2DB(payload);
                    
					Date date = new Date();
			        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//设置日期格式
                    System.out.println(df.format(date)+"数据已保存");
                }

				@Override
				public void connectComplete(boolean arg0, String arg1) {
					Date date = new Date();
					SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//设置日期格式
					System.out.println(df.format(date)+"["+host+"] 连接成功，开始订阅主题 " + myTopic + " qos：" + qos + "...");
					if(conn==null) {
						System.out.println(df.format(date)+"["+host+"] 重新连接数据库 ...");
						conn = new DBConn();
					}
					
					subscribe(client, myTopic, qos);
				}
            });
        	return client;
        } catch (MqttException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public void connect2Server(String topic, int qos) {
    	MqttClient client = createClient(topic, qos);
    	if(client == null) {
    		 new RuntimeException("客户端对象为空");
    	}
    	try {
			client.connect();
		} catch (MqttSecurityException e) {
			e.printStackTrace();
		} catch (MqttException e) {
			e.printStackTrace();
		}
    }
    
    public void reconnect(String topic, int qos) {
    	connect2Server(topic, qos);
    }
    
    
    
    /*
     * 订阅
     */
    public void subscribe(MqttClient client, String topicFilters, int qos) {
        try {
        	System.out.println("主题：" + topicFilters + " qos: " + qos);
            client.subscribe(topicFilters, qos);
        } catch (MqttException e) {
            e.printStackTrace();
        }// 订阅主题

    }
    
    /*
     * 数据持久化
     */
    private void save2DB(Map<String, String> payload) {
    	String saveDetailInfoSql = "insert into `mqtt`(`x_coord`,`y_coord`,`z_coord`,`ip`,`a_speed`,`a_load`,`a_feed`,`x_coord1`,`y_coord1`,`z_coord1`,`b_feed`,`publish_date`,`publish_time`,`b_Num`,`b_ToosNum`,`b_CNCapp`,`b_Runtime`,`state`) " + 
        		"values ('" + payload.get("x_coord") + "','" + payload.get("y_coord") + "','" + payload.get("z_coord") + 
        		"','" + payload.get("ip") + "','" + payload.get("a_speed") + "','" + payload.get("a_load") + 
        		"','" + payload.get("a_feed") + "','" + payload.get("x_coord1") + "','" + payload.get("y_coord1") + 
        		"','" + payload.get("z_coord1") + "','" + payload.get("b_feed") + "',current_date(),current_time()" +
        		",'" + payload.get("b_Num") + "','" + payload.get("b_ToosNum") + "','" + payload.get("b_CNCapp") + "','" + payload.get("b_Runtime") +
        		"','" + payload.get("state") + "')";
        String updateStateSql = "update `device` set `state`='" + payload.get("state") + "' where `ip`='" + payload.get("ip") + "'";
    	//System.out.println(saveDetailInfoSql);
    	//System.out.println(updateStateSql);
    	
    	conn.executeSql(saveDetailInfoSql);
    	conn.executeSql(updateStateSql);
    }
    
    private Map<String,String> parsePayload(MqttMessage payload) throws UnsupportedEncodingException {
    	// 解析接收的payload数据
        Map<String, String> payloadMap = new HashMap();
    	
        String msg = new String(payload.getPayload());
        String tmp = msg.replace(" ","").replace(":{", "|{").split("\\|")[1].replace("{","").replace("}","").replace("\\s","").replace("\n","");	// 删除回车换行等空白符
        //System.out.println(msg);
        String tmp1[] = tmp.split(",");
        
        boolean failed = true;
        for(int i = 0; i < tmp1.length; i ++) {		// 将payload解析保存到Map中
            tmp1[i] = tmp1[i].replace("\"", "");
        	String tmp2[] = tmp1[i].split(":");
        	if (tmp2[1]==null || tmp2[1].equals("")) {
        		failed = false;		// 无法接受到正确的数据
        		continue;
        	}
        	tmp2[1] = new String (tmp2[1].getBytes("GBK"),"UTF-8");		// 转换编码
        	//System.out.println(tmp2[0] + " -- " + tmp2[1]);
        	payloadMap.put(tmp2[0], tmp2[1]);
        	//System.out.println(tmp2[0] + " -- " + tmp2[1]);
        }
        
        if(!failed) {
        	System.out.println("接收的数据有误 ... 停止解析 ...");
        	return null;
        }
		
        // 日志输出
	    //System.out.println(payloadMap); 
        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//设置日期格式
	    System.out.print(df.format(new Date()));
	    for (Map.Entry<String, String> entry : payloadMap.entrySet()) {
		   System.out.print(entry.getKey() + "=" + entry.getValue() + " | "); 
	    }
	    System.out.print("\n");
		 
	    return payloadMap;
    }
    
}
