package com.kisline.processors.base;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Relationship;

import java.util.*;

public enum ConfigUtil {
  INSTANCE;

  private static List<PropertyDescriptor> properties;
  private static Set<Relationship> relationShips;

  public static final PropertyDescriptor OUTPUT = new PropertyDescriptor.Builder().build();

  public static final Relationship JSON =
      new Relationship.Builder()
          .name("json")
          .description("File processes as JSON successfully routed here")
          .build();

  public static final Relationship XML =
      new Relationship.Builder()
          .name("xml")
          .description("File processes as XML successfully routed here")
          .build();

  static {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(OUTPUT);
    ConfigUtil.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(JSON);
    relationships.add(XML);
    ConfigUtil.relationShips = Collections.unmodifiableSet(relationships);
  }

  public static List<PropertyDescriptor> getProperties() {
    return properties;
  }

  public static Set<Relationship> getRelationShips() {
    return relationShips;
  }
}
