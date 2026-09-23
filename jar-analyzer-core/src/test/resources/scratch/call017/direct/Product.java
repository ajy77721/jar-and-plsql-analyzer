package demo;

import org.springframework.data.elasticsearch.annotations.Document;

@Document(indexName = "products_index")
public class Product {}
