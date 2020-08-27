import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;

public class AdminClientConfigTest {

  public static void main(String args[]) throws Exception {
    String topic = "test-topic";
    Properties config = new Properties();
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "dataplatform:9092,dataplatform:9093,dataplatform:9094");
    AdminClient admin = KafkaAdminClient.create(config);

    ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);

    System.out.println("Getting topic "+topic+" configuration");
    DescribeConfigsResult describeResult = admin.describeConfigs(Collections.singleton(topicResource));
    Map<ConfigResource, Config> topicConfig = describeResult.all().get();
    Config c = topicConfig.get(topicResource);
    System.out.println(c.get("message.timestamp.type").value());
  }

}
