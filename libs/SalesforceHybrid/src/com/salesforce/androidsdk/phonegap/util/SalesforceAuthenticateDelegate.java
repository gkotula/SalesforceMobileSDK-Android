package com.salesforce.androidsdk.phonegap.util;

import androidx.activity.ComponentActivity;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.lifecycle.DefaultLifecycleObserver;
import androidx.lifecycle.LifecycleOwner;

import com.salesforce.androidsdk.app.SalesforceSDKManager;
import com.salesforce.androidsdk.rest.ClientManager;
import com.salesforce.androidsdk.rest.RestClient;

public final class SalesforceAuthenticateDelegate {
    public static void authenticate(

    )
    public static void authenticate(
            @NonNull final ComponentActivity ownerActivity,
            @NonNull final RestClientCallback2 callback,
            @NonNull final ClientManager clientManager
    ) {
        ownerActivity.runOnUiThread(() -> {
            final SalesforceSDKManager mgr = SalesforceSDKManager.getInstance();
            final ClientManager cm = new ClientManager(
                    mgr.getAppContext(),
                    mgr.getAccountType(),
                    mgr.getLoginOptions(),
                    mgr.shouldLogoutWhenTokenRevoked()
            );
            try {
                callback.onSuccess(cm.peekRestClient());
            } catch (final Exception ex) {
                // TODO log something
                ownerActivity.getLifecycle().addObserver(
                        new SalesforceAuthenticateDelegateImpl(callback)
                );

                cm.launchLoginFlow(ownerActivity);
            }
        });
    }

    private static class SalesforceAuthenticateDelegateImpl implements DefaultLifecycleObserver {

        @Nullable
        private RestClientCallback2 callback;

        private SalesforceAuthenticateDelegateImpl(@NonNull final RestClientCallback2 callback) {
            this.callback = callback;
        }

        @Override
        public void onStart(@NonNull LifecycleOwner owner) {
            final RestClientCallback2 callback = this.callback;
            if (callback != null) {
                try {

                }
            }
        }

        @Override
        public void onDestroy(@NonNull LifecycleOwner owner) {
            cleanup(owner);
        }

        private void cleanup(@NonNull LifecycleOwner owner) {
            owner.getLifecycle().removeObserver(this);
            callback = null;
        }
    }

    public interface RestClientCallback2 {
        default void onSuccess(@NonNull final RestClient client) {
        }

        default void onFailure() {
        }
    }
}
