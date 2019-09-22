package com.msr.elasticsearch.demo;

import com.msr.elasticsearch.demo.pojo.Item;
import com.msr.elasticsearch.demo.repository.ItemRepository;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class ElasticsearchDemoApplicationTests {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Autowired
    private ItemRepository itemRepository;

    /**
     * 创建索引和映射
     */
    @Test
    public void createIndexAndMapping() {
        this.elasticsearchTemplate.createIndex(Item.class);
        this.elasticsearchTemplate.putMapping(Item.class);

    }

    /**
     * 测试保存
     * 更新操作如jpa，pojo对象发生改变，调用save方法就可实现更新
     */
    @Test
    public void testSave() {
        List<Item> list = new ArrayList<>();
        list.add(new Item(1L, "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(2L, "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(3L, "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(4L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(5L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(1L, "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));

        list.add(new Item(6L, "坚果手机R2", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(7L, "华为META20", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(8L, "小米Mix3S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(9L, "荣耀V20", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(10L, "Vivo10", "手机", "vivo", 2799.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(11L, "Oppo10", "手机", "oppo", 2799.00, "http://image.leyou.com/13123.jpg"));
        // save()时保存单个
        // 接收对象集合，实现批量新增
        itemRepository.saveAll(list);

    }

    @Test
    public void testQuery() {
        log.info("===========testFindAll==================");
        Iterable<Item> all = this.itemRepository.findAll();
        all.forEach(item -> {
            System.out.println(item.toString());
        });

        log.info("=============findById==================");
        Optional<Item> byId = this.itemRepository.findById(5L);
        byId.ifPresent(System.out::println);


        log.info("===============findAllById==============");
        List<Long> listId = new ArrayList<>();
        listId.add(1L);
        listId.add(3L);
        listId.add(5L);
        Iterable<Item> allById = this.itemRepository.findAllById(listId);
        allById.forEach(item -> {
            System.out.println(item.toString());
        });

        log.info("=============findBySort==================");
        Iterable<Item> itemIterable = this.itemRepository.findAll(Sort.by("price").descending());
        itemIterable.forEach(item -> {
            System.out.println(item.toString());
        });

        log.info("test find finished!");

    }


    @Test
    public void testSeniorSearch2() {
        //构建查询条件
        MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery("title", "小米");
        //执行
        Iterable<Item> search = this.itemRepository.search(queryBuilder);
        search.forEach(System.out::println);
    }

    /**
     * 桶聚合
     */
    @Test
    public void testAggs() {
        //初始化自定义构建器
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //添加聚合条件
        queryBuilder.addAggregation(AggregationBuilders.terms("brandAgg").field("brand"));
        //添加结果集过滤，不包含任何字段
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{}, null));
        //执行聚合查询
        AggregatedPage<Item> itemPage = (AggregatedPage<Item>) this.itemRepository.search(queryBuilder.build());
        //解析聚合结果集,根据聚合的字段类型，强转成对应的类型
        StringTerms brandAgg = (StringTerms) itemPage.getAggregation("brandAgg");
        List<StringTerms.Bucket> buckets = brandAgg.getBuckets();
        buckets.forEach(bucket -> {
            System.out.println(bucket.getKeyAsString());
            System.out.println(bucket.getDocCount());
        });
    }

    /**
     * 桶内聚合
     */
    @Test
    public void testAggsInner() {
        //初始化自定义构建器
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //添加聚合条件,以及字聚合
        queryBuilder.addAggregation(AggregationBuilders.terms("brandAgg").field("brand")
                .subAggregation(AggregationBuilders.avg("priceAvg").field("price")));
        //添加结果集过滤，不包含任何字段
        queryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{}, null));
        //执行聚合查询
        AggregatedPage<Item> itemPage = (AggregatedPage<Item>) this.itemRepository.search(queryBuilder.build());
        //解析聚合结果集,根据聚合的字段类型，强转成对应的类型
        StringTerms brandAgg = (StringTerms) itemPage.getAggregation("brandAgg");
        List<StringTerms.Bucket> buckets = brandAgg.getBuckets();
        buckets.forEach(bucket -> {
            System.out.println(bucket.getKeyAsString());
            System.out.println(bucket.getDocCount());
            Map<String, Aggregation> stringAggregationMap = bucket.getAggregations().asMap();
            InternalAvg priceAvg = (InternalAvg) stringAggregationMap.get("priceAvg");

            System.out.println(priceAvg.getValue());
        });
    }

}
