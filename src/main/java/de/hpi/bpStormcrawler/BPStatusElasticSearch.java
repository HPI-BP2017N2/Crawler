package de.hpi.bpStormcrawler;

import com.digitalpebble.stormcrawler.elasticsearch.ElasticSearchConnection;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter(AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)

public class BPStatusElasticSearch {
    private ElasticSearchConnection connection;
    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private static final String ESBoltType = "status";
    private static final String ESStatusIndexNameParamName = "es.status.index.name";
    private static final String ESStatusDocTypeParamName = "es.status.doc.type";
    private SearchRequest shopsWithUrlsDiscoveredSearchRequest;

    private String indexName;
    private String docType;

    BPStatusElasticSearch(Map<String, Object> stormConf){
        loadConfiguration(stormConf);
        getConnectionObject(stormConf);
        setShopsWithUrlsDiscoveredSearchRequest(buildShopsWithStatusDiscoveredRequest());
    }

    public List<Long> getFinishedShops() throws IOException {
        SearchResponse response = getConnection().getClient().search(getShopsWithUrlsDiscoveredSearchRequest());
        return extractFinishedShops(response);
    }

    public void close(){
        getConnection().close();
    }


    private void getConnectionObject(Map<String, Object> stormConf) {
        setConnection(ElasticSearchConnection.getConnection(stormConf, ESBoltType));
    }

    private void loadConfiguration(Map<String, Object> stormConf) {
        setIndexName(ConfUtils.getString(stormConf, ESStatusIndexNameParamName, "status"));
        setDocType(ConfUtils.getString(stormConf, ESStatusDocTypeParamName,"doc"));
    }

    private List<Long> extractFinishedShops(SearchResponse response) {
        Terms Shops = response.getAggregations().get("host");
        List<Long> resultList = new ArrayList<>();
        if (Shops != null) {
            for (Terms.Bucket Shop : Shops.getBuckets()) {
                if (isFinishedShop(Shop)) {
                    resultList.add(Shop.getKeyAsNumber().longValue());
                }
            }
        }
        return resultList;

    }

    private Boolean isFinishedShop(Terms.Bucket Shop) {
        Filter filter = Shop.getAggregations().get("discoveredLinks");
        return filter.getDocCount() == 0;
    }


    private SearchRequest buildShopsWithStatusDiscoveredRequest() {
        SearchRequest request = new SearchRequest(getIndexName()).types(getDocType());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(filterShopsWithStatusDiscovered());
        sourceBuilder.size(0);
        sourceBuilder.explain(false);
        request.source(sourceBuilder);
        return request;
    }

    private static AggregationBuilder filterShopsWithStatusDiscovered() {
        //REST QUERY
        //POST status/_search
        //{
        //  "aggs": {
        //      "hostName" :{
        //        "terms": {
        //          "field": "metadata.hostname"
        //        },
        //
        //        "aggs": {
        //          "numberDiscoveredLinks": {
        //            "filter": {
        //              "term": {
        //                "status": "DISCOVERED"
        //              }
        //            }
        //          }
        //        }
        //      }
        //    }
        //    ,"size": 0
        //}
        return AggregationBuilders
                .terms("host")
                .field("metadata.shopId")
                .subAggregation(
                        AggregationBuilders
                                .filter("discoveredLinks", QueryBuilders.termQuery("status","DISCOVERED"))
                );
    }



}
