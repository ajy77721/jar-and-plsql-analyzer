package com.flowpoc.engine;

import com.flowpoc.model.Predicate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class AggregationPipelineParserTest {

    private final AggregationPipelineParser parser = new AggregationPipelineParser();

    @Test
    void matchStageExtractsPredicates() {
        String pipeline = "[{\"$match\":{\"status\":\"active\",\"age\":{\"$gt\":18}}}]";
        AggregationPipelineParser.ParsedPipeline p = parser.parse(pipeline);
        assertThat(p.matchPredicates()).hasSize(2);
        assertThat(p.matchPredicates()).extracting(Predicate::getColumn)
                .containsExactlyInAnyOrder("status", "age");
    }

    @Test
    void groupStageExtractsFields() {
        String pipeline = "[{\"$group\":{\"_id\":\"$category\",\"total\":{\"$sum\":1}}}]";
        AggregationPipelineParser.ParsedPipeline p = parser.parse(pipeline);
        assertThat(p.groupFields()).containsExactly("category");
    }

    @Test
    void groupStageWithObjectId() {
        String pipeline = "[{\"$group\":{\"_id\":{\"city\":\"$city\",\"country\":\"$country\"}}}]";
        AggregationPipelineParser.ParsedPipeline p = parser.parse(pipeline);
        assertThat(p.groupFields()).containsExactlyInAnyOrder("city", "country");
    }

    @Test
    void lookupStageExtractsJoinInfo() {
        String pipeline = "[{\"$lookup\":{\"from\":\"orders\",\"localField\":\"userId\"," +
                "\"foreignField\":\"_id\",\"as\":\"orders\"}}]";
        AggregationPipelineParser.ParsedPipeline p = parser.parse(pipeline);
        assertThat(p.lookupCollection()).isEqualTo("orders");
        assertThat(p.lookupLocalField()).isEqualTo("userId");
        assertThat(p.lookupForeignField()).isEqualTo("_id");
    }

    @Test
    void sortStageExtractsFields() {
        String pipeline = "[{\"$sort\":{\"createdAt\":-1,\"name\":1}}]";
        AggregationPipelineParser.ParsedPipeline p = parser.parse(pipeline);
        assertThat(p.sortFields()).containsExactlyInAnyOrder("createdAt", "name");
    }

    @Test
    void projectStageExtractsFields() {
        String pipeline = "[{\"$project\":{\"name\":1,\"email\":1,\"_id\":0}}]";
        AggregationPipelineParser.ParsedPipeline p = parser.parse(pipeline);
        assertThat(p.projectFields()).containsExactlyInAnyOrder("name", "email");
        assertThat(p.projectFields()).doesNotContain("_id");
    }

    @Test
    void fullPipelineAllStages() {
        String pipeline = """
                [
                  {"$match": {"active": true}},
                  {"$lookup": {"from": "products", "localField": "productId", "foreignField": "_id", "as": "product"}},
                  {"$group": {"_id": "$category", "count": {"$sum": 1}}},
                  {"$sort": {"count": -1}},
                  {"$project": {"category": 1, "count": 1}}
                ]
                """;
        AggregationPipelineParser.ParsedPipeline p = parser.parse(pipeline);
        assertThat(p.matchPredicates()).hasSize(1);
        assertThat(p.lookupCollection()).isEqualTo("products");
        assertThat(p.groupFields()).containsExactly("category");
        assertThat(p.sortFields()).containsExactly("count");
        assertThat(p.projectFields()).containsExactlyInAnyOrder("category", "count");
    }

    @Test
    void nullPipelineReturnsEmpty() {
        AggregationPipelineParser.ParsedPipeline p = parser.parse(null);
        assertThat(p.matchPredicates()).isEmpty();
        assertThat(p.groupFields()).isEmpty();
    }

    @Test
    void emptyArrayReturnsEmpty() {
        AggregationPipelineParser.ParsedPipeline p = parser.parse("[]");
        assertThat(p.matchPredicates()).isEmpty();
    }

    @Test
    void bindParamInMatchStageExtracted() {
        String pipeline = "[{\"$match\":{\"userId\":\"?0\"}}]";
        AggregationPipelineParser.ParsedPipeline p = parser.parse(pipeline);
        assertThat(p.matchPredicates()).hasSize(1);
        List<Predicate> preds = p.matchPredicates();
        assertThat(preds.get(0).isBindParam()).isTrue();
    }
}
