package com.kisline.processors.bitcoin;

import com.kisline.dbcp.HikariCPService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

// Enum은 인스턴스가 여러 개 생기지 않도록 확실하게 보장해주고 복잡한 직렬화나 리플렉션 상황에서도 직렬화가 자동으로 지원된다는 이점이 있다.
// thread safe 를 위해 class가 아닌 enum 으로 선언함
public enum ConfigUtil {
  INSTANCE;

  private static List<PropertyDescriptor> properties;
  private static Set<Relationship> relationships;

  public static final String JSON_RECORDS = "JSON records created";
  public static final String XML_RECORDS = "JSON records created";
  public static final String RECORDS_READ = "CSV records read";

  public static final String JSON_MIME_TYPE = "application/json";
  public static final String XML_MIME_TYPE = "text/xml";

  public static final PropertyDescriptor OUTPUT =
      new PropertyDescriptor.Builder()
          .name("output")
          .displayName("Output format")
          .description("Format of output FlowFiles")
          .allowableValues("XML", "JSON", "DB", "ALL")
          .defaultValue("ALL")
          .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
          .required(true)
          .build();

  public static final PropertyDescriptor DS_PROP =
      new PropertyDescriptor.Builder()
          .name("dbcp")
          .displayName("Database Connection Pool")
          .description("Database Connection Pool")
          .identifiesControllerService(HikariCPService.class)
          .required(true)
          .build();

  // Relationship -> the states options for terminated situation
  public static final Relationship XML =
      new Relationship.Builder()
          .name("xml")
          .description("Files process as XML successfully routed here")
          .build();

  public static final Relationship JSON =
      new Relationship.Builder()
          .name("json")
          .description("Files process as JSON successfully routed here")
          .build();

  public static final Relationship DB =
      new Relationship.Builder()
          .name("db")
          .description("Files process as DB successfully routed here")
          .build();

  public static final Relationship FAILURE =
      new Relationship.Builder()
          .name("failure")
          .description("Files routed here after processing failure")
          .build();

  static {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(OUTPUT);
    properties.add(DS_PROP);
    ConfigUtil.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(JSON);
    relationships.add(XML);
    relationships.add(DB);
    relationships.add(FAILURE);
    ConfigUtil.relationships = Collections.unmodifiableSet(relationships);
  }

  public static List<PropertyDescriptor> getProperties() {
    return properties;
  }

  public static Set<Relationship> getRelationships() {
    return relationships;
  }
}
