package com.flowpoc.layer2;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonObjectId;
import org.bson.types.ObjectId;

import java.util.Collections;

/**
 * Returns safe mock responses for MongoDB write operations that are intentionally
 * not executed against the real database during dynamic flow analysis.
 *
 * Rules:
 *  - Insert ops  → acknowledged result with a generated ObjectId
 *  - Update ops  → acknowledged with 0 matched / 0 modified (no-op)
 *  - Delete ops  → acknowledged with 0 deleted (no-op)
 *  - save(entity) / insert(entity) returning the entity → return the entity unchanged
 *  - void / unknown return types → null
 *
 * This keeps the calling code happy (no NullPointerException on the result) while
 * ensuring the real collection is never touched.
 */
public final class MongoWriteMocks {

    private MongoWriteMocks() {}

    public static Object forReturnType(Class<?> returnType, Object[] args) {
        if (returnType == null || returnType == void.class || returnType == Void.class) {
            return null;
        }

        String typeName = returnType.getSimpleName();
        return switch (typeName) {
            case "InsertOneResult"  ->
                    InsertOneResult.acknowledged(new BsonObjectId(new ObjectId()));
            case "InsertManyResult" ->
                    InsertManyResult.acknowledged(Collections.emptyMap());
            case "UpdateResult"     ->
                    UpdateResult.acknowledged(0L, 0L, null);
            case "DeleteResult"     ->
                    DeleteResult.acknowledged(0L);
            default -> {
                // save(T entity) → return T unchanged so downstream code gets a non-null entity
                if (args != null) {
                    for (Object arg : args) {
                        if (arg != null && returnType.isInstance(arg)) yield arg;
                    }
                }
                yield null;
            }
        };
    }
}
