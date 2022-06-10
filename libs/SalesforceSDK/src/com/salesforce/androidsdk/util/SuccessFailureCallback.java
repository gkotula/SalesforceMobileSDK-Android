package com.salesforce.androidsdk.util;

import androidx.annotation.NonNull;

public interface SuccessFailureCallback<T> {
    default void onSuccess(@NonNull final T result) {
    }

    default void onFailure() {
    }
}
