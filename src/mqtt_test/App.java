package mqtt_test;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;


/**
 * MQTT客户端对象。
 * 封装publish与subscribe方法。
 * 重写messageArrived方法，用于数据持久化。
 */
public class App {
    //private String host = "tcp://218.17.87.115:1883";		// MQTT服务端
    private String host = "tcp://172.16.7.15:1883";		// MQTT服务端
    private String userName = "admin";					// 连接MQTT服务的用户名
    private String passWord = "password";				// 连接MQTT服务的密码
    private MqttClient client;							// MQTT客户端类对象
    private String id;									// MQTT客户端的ID
    private MqttTopic mqttTopic;						// 订阅主题对象
    private String myTopic = "51220";					// 默认的订阅主题
    private MqttMessage message;						// MQTT传输数据格式对象
    
    private DBConn conn = new DBConn();
    
    public App(String id, String[] topics, int[] qos) {
    	// 构造方法，调用App(id,callback,cleanSession)构造方法
        this(id, null, true, topics, qos);
    }
    
    public App(String id, MqttCallback callback, boolean cleanSession, String[] topics, int[] qos){
    	// 构造方法
        try {
            client = new MqttClient(host, id, new MemoryPersistence());		// id应该保持唯一性
            MqttConnectOptions options = new MqttConnectOptions();			// MQTT客户端连接设置对象
            options.setConnectionTimeout(10);		// 连接超时时间 
            options.setKeepAliveInterval(20000);		// 连接保持时间
            options.setAutomaticReconnect(true);	// 设置自动重连（版本1.2）
            options.setCleanSession(cleanSession);	// 不清除session，以便重连后可以继续接受数据
            if (callback == null) {
                client.setCallback(new MqttCallbackExtended() {		
                	// 重写客户端回调方法，此处实现MqttCallbackExtended接口配合setAutomaticReconnect达到断开重连重新订阅的功能
                    @Override
                    public void connectionLost(Throwable arg0) {
                    	// 连接丢失时的执行动作
						Date date = new Date();
				        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//设置日期格式
                        System.out.println(df.format(date) + id + " 连接异常断开  " + arg0);
                        if(conn != null) {
                            //conn.closeDB();
                        }
                    }
    
                    @Override
                    public void deliveryComplete(IMqttDeliveryToken arg0) {
                    	// 接收到已经发布的 QoS 1 或 QoS 2 消息的传递令牌时执行的动作
                        System.out.println(id + " deliveryComplete " + arg0);
                        if(conn != null) {
                            //conn.closeDB();
                        }
                    }
    
                    @Override
                    public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
                    	// 接收订阅的消息后执行的动作，当MQTT客户端对象执行subscribe方法后执行
                        System.out.println(id + " messageArrived: " + arg1.toString());
                        
                        // 解析订阅频道信息与数据持久化
                        Map<String,String> payload = new HashMap();
                        //System.out.println("开始解析数据");
                        payload = parsePayload(arg1);
                        if(payload == null) {
                        	System.out.println("数据有误，停止数据保存操作 ...");
                        	return;
                        }
                        //System.out.println("开始保存数据");
                        save2DB(payload);
                        
						Date date = new Date();
				        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//设置日期格式
                        System.out.println(df.format(date)+"数据已保存");
                    }

					@Override
					public void connectComplete(boolean arg0, String arg1) {
						// 该方法由MqttCallbackExtended接口（MqttCallback接口的拓展）提供，连接成功后执行的操作（配合setAutomaticReconnect，针对断开重连设置）
						try {
							Date date = new Date();
					        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//设置日期格式
							System.out.println(df.format(date)+"["+host+"] 连接成功，开始订阅主题 " + topics[0].toString() + " qos：" + qos[0] + "...");
							if(conn==null) {
								System.out.println(df.format(date)+"["+host+"] 重新连接数据库 ...");
								conn = new DBConn();
							}
							client.subscribe(topics, qos);
						} catch (MqttException e) {
							System.out.println(e);
						}
					}
                });
            } else {
                client.setCallback(callback);
            }
            client.connect(options);
        } catch (MqttException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
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
        	//tmp2[1] = new String (tmp2[1].getBytes("GBK"),"UTF-8");		// 转换编码
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
    
    private void save2DB(Map<String, String> payload) {
    	// 数据持久化
    	String saveDetailInfoSql = "insert into `mqtt_copy2`(`x_coord`,`y_coord`,`z_coord`,`ip`,`a_speed`,`a_load`,`a_feed`,`x_coord1`,`y_coord1`,`z_coord1`,`b_feed`,`publish_date`,`publish_time`,`b_Num`,`b_ToosNum`,`b_CNCapp`,`b_Runtime`,`state`) " + 
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
    
    public void sendMessage(String msg) {
        sendMessage(myTopic, msg);
    }
    
    public void sendMessage(String topic, String msg){
        try {
            message = new MqttMessage();
            message.setQos(1);
            message.setRetained(true);
            message.setPayload(msg.getBytes());
            mqttTopic = client.getTopic(topic);
            MqttDeliveryToken token = mqttTopic.publish(message);//发布主题
            token.waitForCompletion();
        } catch (MqttPersistenceException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        } catch (MqttException e) {
            // TODO 自动生成的 catch 块
            e.printStackTrace();
        }
    }
    
    public void subscribe(String[] topicFilters, int[] qos) {
        try {
        	System.out.println("主题：" + topicFilters + " qos: " + qos.toString());
            client.subscribe(topicFilters, qos);
        } catch (MqttException e) {
            e.printStackTrace();
        }// 订阅主题

    }
    
}