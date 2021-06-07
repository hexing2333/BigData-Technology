package org.zkpk.hbase.inputSource;
import java.io.IOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class MemberMapper extends TableMapper<Writable,Writable>{
	private Text k = new Text();
	private Text v = new Text();
	public static final String FIELD_COMMON_SEPARATOR="\u0001";
	@Override
	protected void setup(Context context) throws IOException ,InterruptedException{
		
	}
	
	@Override 
	public void map(ImmutableBytesWritable row, Result columns, 
			Context context) throws IOException ,InterruptedException {
		String value = null;
		
		//\u884c\u952e\u503c
		String rowkey = new String(row.get());
		
		//\u4e00\u884c\u4e2d\u7684\u6240\u6709\u5217\u65cf
		byte[] columnFamily = null;
		
		//\u4e00\u884c\u4e2d\u7684\u6240\u6709\u5217\u540d
		byte[] columnQualifier = null;
		
		long ts = 0L;
		
		try{
			for(KeyValue kv : columns.list()){
				value = Bytes.toStringBinary(kv.getValue());
				//\u83b7\u5f97\u4e00\u884c\u4e2d\u7684\u6240\u6709\u7684\u5217\u65cf
				columnFamily = kv.getFamily();
				//\u83b7\u5f97\u4e00\u884c\u4e2d\u7684\u6240\u6709\u5217
				columnQualifier = kv.getQualifier();
				//\u83b7\u5f97\u5355\u5143\u7684\u65f6\u95f4\u6233
				ts = kv.getTimestamp();
					
					k.set(rowkey);
					v.set(Bytes.toString(columnFamily)+FIELD_COMMON_SEPARATOR+Bytes.toString(columnQualifier)
						+FIELD_COMMON_SEPARATOR+value+FIELD_COMMON_SEPARATOR+ts);
					context.write(k, v);
			}
		}catch(Exception e){
			e.printStackTrace();
			System.err.println("Error:"+e.getMessage()+",Row:"+Bytes.toString(row.get())+",Value:"+value);
		}
	};
}
