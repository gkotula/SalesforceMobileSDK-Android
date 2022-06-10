package com.salesforce.androidsdk.util;

import androidx.activity.ComponentActivity;
import androidx.activity.result.ActivityResultRegistry;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.lifecycle.DefaultLifecycleObserver;
import androidx.lifecycle.LifecycleOwner;

import com.salesforce.androidsdk.app.SalesforceSDKManager;
import com.salesforce.androidsdk.rest.ClientManager;

public final class LifecycleRestClientBuilder implements DefaultLifecycleObserver {
    public static void buildClient(@NonNull final ComponentActivity activity) {
        final SalesforceSDKManager mgr = SalesforceSDKManager.getInstance();
        final ClientManager cm = new ClientManager(
                mgr.getAppContext(),
                mgr.getAccountType(),
                mgr.getLoginOptions(),
                mgr.shouldLogoutWhenTokenRevoked()
        );
    }

    @Nullable
    private ComponentActivity activity;

    private LifecycleRestClientBuilder(@NonNull final ComponentActivity activity) {
        this.activity = activity;
        activity.getLifecycle().addObserver(this);
    }

    @Override
    public void onDestroy(@NonNull LifecycleOwner owner) {
        // TODO
    }
}
