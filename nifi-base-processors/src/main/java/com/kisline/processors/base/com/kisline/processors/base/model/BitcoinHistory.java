package com.kisline.processors.base.com.kisline.processors.base.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Objects;

@XmlRootElement(name = "history")
@XmlAccessorType(XmlAccessType.FIELD)
public class BitcoinHistory implements Serializable {
  private static final long serialVersionUID = -1;

  @XmlElement(name = "timestamp")
  @XmlJavaTypeAdapter(ZonedDateTimeAdapter.class)
  @JsonProperty("timestamp")
  private ZonedDateTime timestamp;

  @XmlElement(name = "open")
  @JsonProperty("open")
  private double open;

  @XmlElement(name = "close")
  @JsonProperty("close")
  private double close;

  @XmlElement(name = "high")
  @JsonProperty("high")
  private double high;

  @XmlElement(name = "low")
  @JsonProperty("low")
  private double low;

  @XmlElement(name = "btc-volume")
  @JsonProperty("btcVolume")
  private double btcVolume;

  @XmlElement(name = "weighted-price")
  @JsonProperty("weightedPrice")
  private double weightedPrice;

  @XmlElement(name = "usd-volume")
  @JsonProperty("usdVolume")
  private double usdVolume;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BitcoinHistory that = (BitcoinHistory) o;
    return Double.compare(that.open, open) == 0
        && Double.compare(that.close, close) == 0
        && Double.compare(that.high, high) == 0
        && Double.compare(that.low, low) == 0
        && Double.compare(that.btcVolume, btcVolume) == 0
        && Double.compare(that.weightedPrice, weightedPrice) == 0
        && Double.compare(that.usdVolume, usdVolume) == 0
        && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, open, close, high, low, btcVolume, weightedPrice, usdVolume);
  }

  @Override
  public String toString() {
    return "BitcoinHistory{"
        + "timestamp="
        + timestamp
        + ", open="
        + open
        + ", close="
        + close
        + ", high="
        + high
        + ", low="
        + low
        + ", btcVolume="
        + btcVolume
        + ", weightedPrice="
        + weightedPrice
        + ", usdVolume="
        + usdVolume
        + '}';
  }

  public ZonedDateTime getTimestamp() {
    return timestamp;
  }

  public double getOpen() {
    return open;
  }

  public double getClose() {
    return close;
  }

  public double getHigh() {
    return high;
  }

  public double getLow() {
    return low;
  }

  public double getBtcVolume() {
    return btcVolume;
  }

  public double getWeightedPrice() {
    return weightedPrice;
  }

  public double getUsdVolume() {
    return usdVolume;
  }

  public void setTimestamp(ZonedDateTime timestamp) {
    this.timestamp = timestamp;
  }

  public void setOpen(double open) {
    this.open = open;
  }

  public void setClose(double close) {
    this.close = close;
  }

  public void setHigh(double high) {
    this.high = high;
  }

  public void setLow(double low) {
    this.low = low;
  }

  public void setBtcVolume(double btcVolume) {
    this.btcVolume = btcVolume;
  }

  public void setWeightedPrice(double weightedPrice) {
    this.weightedPrice = weightedPrice;
  }

  public void setUsdVolume(double usdVolume) {
    this.usdVolume = usdVolume;
  }
}
