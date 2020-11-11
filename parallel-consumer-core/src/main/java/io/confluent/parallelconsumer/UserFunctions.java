package io.confluent.parallelconsumer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.function.Function;

@RequiredArgsConstructor
public class UserFunction<A, B> implements Function<A, B> {

    private final Function<A, B> wrappedFunction;

    public static <A, B> B apply(final Function<A, B> wrappedFunction, A a) {
        try {
            return wrappedFunction.apply(a);
        } catch (Exception e) {
            throw new ErrorInUserFunctionException("Error occurred in code supplied by user", e);
        }
        return null;
    }

    @Override
    public B apply(final A a) {
        try {
            return wrappedFunction.apply(a);
        } catch (Exception e) {
            throw new ErrorInUserFunctionException("Error occurred in code supplied by user", e);
        }
    }
}
