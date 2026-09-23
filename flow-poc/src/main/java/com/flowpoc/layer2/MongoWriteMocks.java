/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  com.mongodb.client.result.DeleteResult
 *  com.mongodb.client.result.InsertManyResult
 *  com.mongodb.client.result.InsertOneResult
 *  com.mongodb.client.result.UpdateResult
 *  org.bson.BsonObjectId
 *  org.bson.BsonValue
 *  org.bson.types.ObjectId
 */
package com.flowpoc.layer2;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import java.util.Collections;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

public final class MongoWriteMocks {
    private MongoWriteMocks() {
    }

    public static Object forReturnType(Class<?> returnType, Object[] args) {
        String typeName;
        if (returnType == null || returnType == Void.TYPE || returnType == Void.class) {
            return null;
        }
        return switch (typeName = returnType.getSimpleName()) {
            case "InsertOneResult" -> InsertOneResult.acknowledged((BsonValue)new BsonObjectId(new ObjectId()));
            case "InsertManyResult" -> InsertManyResult.acknowledged(Collections.emptyMap());
            case "UpdateResult" -> UpdateResult.acknowledged((long)0L, (Long)0L, null);
            case "DeleteResult" -> DeleteResult.acknowledged((long)0L);
            default -> {
                if (args != null) {
                    for (Object arg : args) {
                        if (arg == null || !returnType.isInstance(arg)) continue;
                        yield arg;
                    }
                }
                yield null;
            }
        };
    }
}

