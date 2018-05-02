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
    private SearchRequest domainsWithUrlsDiscoveredSearchRequest;

    private String indexName;
    private String docType;

    BPStatusElasticSearch(Map<String, Object> stormConf){
        loadConfiguration(stormConf);
        getConnectionObject(stormConf);
        setDomainsWithUrlsDiscoveredSearchRequest(buildDomainsWithStatusDiscoveredRequest());
    }

    public List<String> getFinishedDomains() throws IOException {
        SearchResponse response = getConnection().getClient().search(getDomainsWithUrlsDiscoveredSearchRequest());
        return extractFinishedDomains(response);
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

    private List<String> extractFinishedDomains(SearchResponse response) {
        Terms domains = response.getAggregations().get("hostName");
        List<String> resultList = new ArrayList<>();
        if (domains != null) {
            for (Terms.Bucket domain : domains.getBuckets()) {
                if (isFinishedDomain(domain)) {
                    resultList.add(domain.getKeyAsString());
                }
            }
        }
        return resultList;

    }

    private Boolean isFinishedDomain(Terms.Bucket domain) {
        Filter filter = domain.getAggregations().get("discoveredLinks");
        return filter.getDocCount() == 0;
    }


    private SearchRequest buildDomainsWithStatusDiscoveredRequest() {
        SearchRequest request = new SearchRequest(getIndexName()).types(getDocType());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(filterDomainsWithStatusDiscovered());
        sourceBuilder.size(0);
        sourceBuilder.explain(false);
        request.source(sourceBuilder);
        return request;
    }

    private static AggregationBuilder filterDomainsWithStatusDiscovered() {
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
                .terms("hostName")
                .field("metadata.hostname")
                .subAggregation(
                        AggregationBuilders
                                .filter("discoveredLinks", QueryBuilders.termQuery("status","DISCOVERED"))
                );
    }



}
