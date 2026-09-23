package demo;

import org.springframework.data.elasticsearch.annotations.Document;

@Document(indexName = "catalog_index")
public class Catalog {}
