package com.msr.elasticsearch.demo.repository;

import com.msr.elasticsearch.demo.pojo.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

/**
 * @Description:
 * @Author: maishuren
 * @Date: 2019/9/22 10:59
 */
public interface ItemRepository extends ElasticsearchRepository<Item, Long> {


    /**
     * 和jpa自定义查询相似
     * 自定义接口方法
     *
     * @param title 标题
     * @return 返回结果
     */
    List<Item> findByTitle(String title);
}
