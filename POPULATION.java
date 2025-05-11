// ORM class for table 'POPULATION'
// WARNING: This class is AUTO-GENERATED. Modify at your own risk.
//
// Debug information:
// Generated date: Sat May 10 03:54:38 UTC 2025
// For connector: org.apache.sqoop.manager.MySQLManager
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class POPULATION extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  public static interface FieldSetterCommand {    void setField(Object value);  }  protected ResultSet __cur_result_set;
  private Map<String, FieldSetterCommand> setters = new HashMap<String, FieldSetterCommand>();
  private void init0() {
    setters.put("khu_vuc", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        POPULATION.this.khu_vuc = (String)value;
      }
    });
    setters.put("dan_so", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        POPULATION.this.dan_so = (Integer)value;
      }
    });
    setters.put("dien_tich", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        POPULATION.this.dien_tich = (Float)value;
      }
    });
    setters.put("mat_do_dan_so", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        POPULATION.this.mat_do_dan_so = (Float)value;
      }
    });
    setters.put("vung", new FieldSetterCommand() {
      @Override
      public void setField(Object value) {
        POPULATION.this.vung = (String)value;
      }
    });
  }
  public POPULATION() {
    init0();
  }
  private String khu_vuc;
  public String get_khu_vuc() {
    return khu_vuc;
  }
  public void set_khu_vuc(String khu_vuc) {
    this.khu_vuc = khu_vuc;
  }
  public POPULATION with_khu_vuc(String khu_vuc) {
    this.khu_vuc = khu_vuc;
    return this;
  }
  private Integer dan_so;
  public Integer get_dan_so() {
    return dan_so;
  }
  public void set_dan_so(Integer dan_so) {
    this.dan_so = dan_so;
  }
  public POPULATION with_dan_so(Integer dan_so) {
    this.dan_so = dan_so;
    return this;
  }
  private Float dien_tich;
  public Float get_dien_tich() {
    return dien_tich;
  }
  public void set_dien_tich(Float dien_tich) {
    this.dien_tich = dien_tich;
  }
  public POPULATION with_dien_tich(Float dien_tich) {
    this.dien_tich = dien_tich;
    return this;
  }
  private Float mat_do_dan_so;
  public Float get_mat_do_dan_so() {
    return mat_do_dan_so;
  }
  public void set_mat_do_dan_so(Float mat_do_dan_so) {
    this.mat_do_dan_so = mat_do_dan_so;
  }
  public POPULATION with_mat_do_dan_so(Float mat_do_dan_so) {
    this.mat_do_dan_so = mat_do_dan_so;
    return this;
  }
  private String vung;
  public String get_vung() {
    return vung;
  }
  public void set_vung(String vung) {
    this.vung = vung;
  }
  public POPULATION with_vung(String vung) {
    this.vung = vung;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof POPULATION)) {
      return false;
    }
    POPULATION that = (POPULATION) o;
    boolean equal = true;
    equal = equal && (this.khu_vuc == null ? that.khu_vuc == null : this.khu_vuc.equals(that.khu_vuc));
    equal = equal && (this.dan_so == null ? that.dan_so == null : this.dan_so.equals(that.dan_so));
    equal = equal && (this.dien_tich == null ? that.dien_tich == null : this.dien_tich.equals(that.dien_tich));
    equal = equal && (this.mat_do_dan_so == null ? that.mat_do_dan_so == null : this.mat_do_dan_so.equals(that.mat_do_dan_so));
    equal = equal && (this.vung == null ? that.vung == null : this.vung.equals(that.vung));
    return equal;
  }
  public boolean equals0(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof POPULATION)) {
      return false;
    }
    POPULATION that = (POPULATION) o;
    boolean equal = true;
    equal = equal && (this.khu_vuc == null ? that.khu_vuc == null : this.khu_vuc.equals(that.khu_vuc));
    equal = equal && (this.dan_so == null ? that.dan_so == null : this.dan_so.equals(that.dan_so));
    equal = equal && (this.dien_tich == null ? that.dien_tich == null : this.dien_tich.equals(that.dien_tich));
    equal = equal && (this.mat_do_dan_so == null ? that.mat_do_dan_so == null : this.mat_do_dan_so.equals(that.mat_do_dan_so));
    equal = equal && (this.vung == null ? that.vung == null : this.vung.equals(that.vung));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.khu_vuc = JdbcWritableBridge.readString(1, __dbResults);
    this.dan_so = JdbcWritableBridge.readInteger(2, __dbResults);
    this.dien_tich = JdbcWritableBridge.readFloat(3, __dbResults);
    this.mat_do_dan_so = JdbcWritableBridge.readFloat(4, __dbResults);
    this.vung = JdbcWritableBridge.readString(5, __dbResults);
  }
  public void readFields0(ResultSet __dbResults) throws SQLException {
    this.khu_vuc = JdbcWritableBridge.readString(1, __dbResults);
    this.dan_so = JdbcWritableBridge.readInteger(2, __dbResults);
    this.dien_tich = JdbcWritableBridge.readFloat(3, __dbResults);
    this.mat_do_dan_so = JdbcWritableBridge.readFloat(4, __dbResults);
    this.vung = JdbcWritableBridge.readString(5, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void loadLargeObjects0(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(khu_vuc, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(dan_so, 2 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeFloat(dien_tich, 3 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeFloat(mat_do_dan_so, 4 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeString(vung, 5 + __off, 12, __dbStmt);
    return 5;
  }
  public void write0(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeString(khu_vuc, 1 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeInteger(dan_so, 2 + __off, 4, __dbStmt);
    JdbcWritableBridge.writeFloat(dien_tich, 3 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeFloat(mat_do_dan_so, 4 + __off, 7, __dbStmt);
    JdbcWritableBridge.writeString(vung, 5 + __off, 12, __dbStmt);
  }
  public void readFields(DataInput __dataIn) throws IOException {
this.readFields0(__dataIn);  }
  public void readFields0(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.khu_vuc = null;
    } else {
    this.khu_vuc = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.dan_so = null;
    } else {
    this.dan_so = Integer.valueOf(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.dien_tich = null;
    } else {
    this.dien_tich = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.mat_do_dan_so = null;
    } else {
    this.mat_do_dan_so = Float.valueOf(__dataIn.readFloat());
    }
    if (__dataIn.readBoolean()) { 
        this.vung = null;
    } else {
    this.vung = Text.readString(__dataIn);
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.khu_vuc) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, khu_vuc);
    }
    if (null == this.dan_so) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.dan_so);
    }
    if (null == this.dien_tich) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.dien_tich);
    }
    if (null == this.mat_do_dan_so) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.mat_do_dan_so);
    }
    if (null == this.vung) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, vung);
    }
  }
  public void write0(DataOutput __dataOut) throws IOException {
    if (null == this.khu_vuc) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, khu_vuc);
    }
    if (null == this.dan_so) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeInt(this.dan_so);
    }
    if (null == this.dien_tich) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.dien_tich);
    }
    if (null == this.mat_do_dan_so) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeFloat(this.mat_do_dan_so);
    }
    if (null == this.vung) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, vung);
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(khu_vuc==null?"null":khu_vuc, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dan_so==null?"null":"" + dan_so, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dien_tich==null?"null":"" + dien_tich, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(mat_do_dan_so==null?"null":"" + mat_do_dan_so, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(vung==null?"null":vung, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  public void toString0(DelimiterSet delimiters, StringBuilder __sb, char fieldDelim) {
    __sb.append(FieldFormatter.escapeAndEnclose(khu_vuc==null?"null":khu_vuc, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dan_so==null?"null":"" + dan_so, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(dien_tich==null?"null":"" + dien_tich, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(mat_do_dan_so==null?"null":"" + mat_do_dan_so, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(vung==null?"null":vung, delimiters));
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.khu_vuc = null; } else {
      this.khu_vuc = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dan_so = null; } else {
      this.dan_so = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dien_tich = null; } else {
      this.dien_tich = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.mat_do_dan_so = null; } else {
      this.mat_do_dan_so = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.vung = null; } else {
      this.vung = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  private void __loadFromFields0(Iterator<String> __it) {
    String __cur_str = null;
    try {
    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.khu_vuc = null; } else {
      this.khu_vuc = __cur_str;
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dan_so = null; } else {
      this.dan_so = Integer.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.dien_tich = null; } else {
      this.dien_tich = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.mat_do_dan_so = null; } else {
      this.mat_do_dan_so = Float.valueOf(__cur_str);
    }

    if (__it.hasNext()) {
        __cur_str = __it.next();
    } else {
        __cur_str = "null";
    }
    if (__cur_str.equals("null")) { this.vung = null; } else {
      this.vung = __cur_str;
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    POPULATION o = (POPULATION) super.clone();
    return o;
  }

  public void clone0(POPULATION o) throws CloneNotSupportedException {
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new HashMap<String, Object>();
    __sqoop$field_map.put("khu_vuc", this.khu_vuc);
    __sqoop$field_map.put("dan_so", this.dan_so);
    __sqoop$field_map.put("dien_tich", this.dien_tich);
    __sqoop$field_map.put("mat_do_dan_so", this.mat_do_dan_so);
    __sqoop$field_map.put("vung", this.vung);
    return __sqoop$field_map;
  }

  public void getFieldMap0(Map<String, Object> __sqoop$field_map) {
    __sqoop$field_map.put("khu_vuc", this.khu_vuc);
    __sqoop$field_map.put("dan_so", this.dan_so);
    __sqoop$field_map.put("dien_tich", this.dien_tich);
    __sqoop$field_map.put("mat_do_dan_so", this.mat_do_dan_so);
    __sqoop$field_map.put("vung", this.vung);
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if (!setters.containsKey(__fieldName)) {
      throw new RuntimeException("No such field:"+__fieldName);
    }
    setters.get(__fieldName).setField(__fieldVal);
  }

}
