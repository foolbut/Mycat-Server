package io.mycat.route.function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import io.mycat.config.model.rule.RuleAlgorithm;

public class PartitionByStringMode extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    private int count;
    @Override
    public void init() {
    
        
    }



    public void setCount(int count) {
        this.count = count == 0?1:count;
    }

    @Override
    public Integer calculate(String columnValue)  {
//      columnValue = NumberParseUtil.eliminateQoute(columnValue);
        try {
            int hashCode = null == columnValue||StringUtils.isBlank(columnValue)?0:columnValue.hashCode();
            if(hashCode < 0) hashCode = 0-hashCode;
            
            return (int)(hashCode%count);
        } catch (NumberFormatException e){
            throw new IllegalArgumentException(new StringBuilder().append("columnValue:").append(columnValue).append(" Please eliminate any quote and non number within it.").toString(),e);
        }

    }
    

    @Override
    public int getPartitionNum() {
        int nPartition = this.count;
        return nPartition;
    }

    private static void hashTest()  {
        PartitionByStringMode hash=new PartitionByStringMode();
        hash.setCount(11);
        hash.init();
        
        int[] bucket=new int[hash.count];
        
        Map<Integer,List<Integer>> hashed=new HashMap<>();
        
        int total=1000_0000;//数据量
        int c=0;
        for(int i=100_0000;i<total+100_0000;i++){//假设分片键从100万开始
            c++;
            int h=hash.calculate(Integer.toString(i));
            bucket[h]++;
            List<Integer> list=hashed.get(h);
            if(list==null){
                list=new ArrayList<>();
                hashed.put(h, list);
            }
            list.add(i);
        }
        System.out.println(c+"   "+total);
        double d=0;
        c=0;
        int idx=0;
        System.out.println("index    bucket   ratio");
        for(int i:bucket){
            d+=i/(double)total;
            c+=i;
            System.out.println(idx+++"  "+i+"   "+(i/(double)total));
        }
        System.out.println(d+"  "+c);
        
        System.out.println("****************************************************");
        rehashTest(hashed.get(0));
    }
    private static void rehashTest(List<Integer> partition)  {
        PartitionByStringMode hash=new PartitionByStringMode();
        hash.count=110;//分片数
        hash.init();
        
        int[] bucket=new int[hash.count];
        
        int total=partition.size();//数据量
        int c=0;
        for(int i:partition){//假设分片键从100万开始
            c++;
            int h=hash.calculate(Integer.toString(i));
            bucket[h]++;
        }
        System.out.println(c+"   "+total);
        c=0;
        int idx=0;
        System.out.println("index    bucket   ratio");
        for(int i:bucket){
            c+=i;
            System.out.println(idx+++"  "+i+"   "+(i/(double)total));
        }
    }
    public static void main(String[] args)  {
//      hashTest();
        PartitionByStringMode partitionByMod = new PartitionByStringMode();
        partitionByMod.count=8;
        System.out.println(partitionByMod.calculate("abc"));
        System.out.println(partitionByMod.calculate("def"));
        System.out.println(partitionByMod.calculate("\'abc\'"));
        System.out.println(partitionByMod.calculate("\"def\""));
        System.out.println(partitionByMod.calculate("\'def\'"));
        
    }

}
