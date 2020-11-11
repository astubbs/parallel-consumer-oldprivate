package io.confluent;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.confluent.TransactionState.State.*;

@Slf4j
public class TransactionState {

    enum State {
        IN_TRANSACTION, NOT_IN_TRANSACTION, NOT_APPLICABLE
    }

    @Getter
    State txState = NOT_APPLICABLE;

    public void setInTransaction() {
        txState = IN_TRANSACTION;
    }

    public void setNotInTransaction() {
        txState = NOT_IN_TRANSACTION;
    }

    public boolean isInTransaction() {
        return txState == IN_TRANSACTION;
    }

}
