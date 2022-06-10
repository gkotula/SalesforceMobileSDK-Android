/*
 * Copyright (c) 2017-present, salesforce.com, inc.
 * All rights reserved.
 * Redistribution and use of this software in source and binary forms, with or
 * without modification, are permitted provided that the following conditions
 * are met:
 * - Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * - Neither the name of salesforce.com, inc. nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission of salesforce.com, inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.salesforce.androidsdk.ui;

import android.app.Activity;
import android.content.IntentFilter;
import android.os.Bundle;
import android.view.KeyEvent;

import androidx.activity.ComponentActivity;
import androidx.annotation.MainThread;
import androidx.annotation.NonNull;
import androidx.lifecycle.DefaultLifecycleObserver;
import androidx.lifecycle.LifecycleOwner;
import androidx.savedstate.SavedStateRegistry;

import com.salesforce.androidsdk.accounts.UserAccountManager;
import com.salesforce.androidsdk.app.SalesforceSDKManager;
import com.salesforce.androidsdk.rest.ClientManager;
import com.salesforce.androidsdk.rest.RestClient;
import com.salesforce.androidsdk.util.EventsObservable;
import com.salesforce.androidsdk.util.LogoutCompleteReceiver;
import com.salesforce.androidsdk.util.SalesforceSDKLogger;
import com.salesforce.androidsdk.util.UserSwitchReceiver;

/**
 * Class taking care of common behavior of Salesforce*Activity classes
 */

public class SalesforceActivityDelegate {

    private final Activity activity;
    private UserSwitchReceiver userSwitchReceiver;
    private LogoutCompleteReceiver logoutCompleteReceiver;
    private boolean hasLaunchedLoginFlowFromPeekRestClientFailure = false;

    public SalesforceActivityDelegate(Activity activity) {
        this.activity = activity;

        if (activity instanceof ComponentActivity) {
            final ComponentActivity parent = (ComponentActivity) activity;
            parent.getLifecycle().addObserver(new ParentActivityLifecycleObserver(parent));
        }
    }

    private class ParentActivityLifecycleObserver implements DefaultLifecycleObserver {
        @NonNull
        private final SavedStateRegistry.SavedStateProvider savedStateProvider;

        @NonNull
        private final SavedStateRegistry savedStateRegistry;

        public ParentActivityLifecycleObserver(@NonNull final ComponentActivity parent) {
            this.savedStateProvider = () -> {
                SalesforceSDKLogger.d(TAG, "SavedStateProvider called.");
                final Bundle bundle = new Bundle();
                bundle.putBoolean(KEY_HAS_STARTED_LOGIN_FLOW, hasLaunchedLoginFlowFromPeekRestClientFailure);
                return bundle;
            };
            this.savedStateRegistry = parent.getSavedStateRegistry();
        }

        @Override
        public void onCreate(@NonNull LifecycleOwner owner) {
            // TODO explain why we need to do it this way instead of in the delegate's onCreate()
            savedStateRegistry.registerSavedStateProvider(KEY_SAVED_STATE_PROVIDER, savedStateProvider);
            final Bundle stored = savedStateRegistry.consumeRestoredStateForKey(KEY_SAVED_STATE_PROVIDER);

            if (stored != null) {
                hasLaunchedLoginFlowFromPeekRestClientFailure =
                        stored.getBoolean(KEY_HAS_STARTED_LOGIN_FLOW, false);
            }
        }

        @Override
        public void onDestroy(@NonNull LifecycleOwner owner) {
            owner.getLifecycle().removeObserver(this);
        }
    }

    public void onCreate() {
        userSwitchReceiver = new ActivityUserSwitchReceiver();
        activity.registerReceiver(userSwitchReceiver, new IntentFilter(UserAccountManager.USER_SWITCH_INTENT_ACTION));
        logoutCompleteReceiver = new ActivityLogoutCompleteReceiver();
        activity.registerReceiver(logoutCompleteReceiver, new IntentFilter(SalesforceSDKManager.LOGOUT_COMPLETE_INTENT_ACTION));

        // Lets observers know that activity creation is complete.
        EventsObservable.get().notifyEvent(EventsObservable.EventType.MainActivityCreateComplete, this);
    }

    /**
     * If {@code buildRestClient} is false, this method simply calls this Activity's
     * {@link SalesforceActivityInterface#onResume(RestClient)} with {@code null}.
     * <p>
     * If {@code buildRestClient = true}, this attempts to build an authenticated {@link RestClient} for
     * the current active user, launching the login flow if building it fails. Once the login flow
     * has succeeded and this method is called again during your Activity's {@link Activity#onResume()},
     * the {@link RestClient} will be built and your Activity's {@link SalesforceActivityInterface#onResume(RestClient)}
     * method will be called with the authenticated {@link RestClient}.
     * <p>
     * If the login flow fails or the user otherwise leaves the flow before successfully authenticating,
     * the second call to this method with {@code buildRestClient = true} from {@link Activity#onResume()}
     * will automatically start the logout process, restarting the app back to the Main Activity if
     * no other user is logged in.
     *
     * @param buildRestClient True if you want the delegate to try building the authenticated {@link RestClient} (possibly kicking off the login flow to do so), false otherwise.
     */
    @MainThread
    public void onResume(boolean buildRestClient) {
        if (buildRestClient) {
            // Don't use SalesforceSDKManager.getInstance().getClientManager() because that does not
            // respect the current shouldLogoutWhenTokenRevoked flag.
            final SalesforceSDKManager mgr = SalesforceSDKManager.getInstance();
            final ClientManager cm = new ClientManager(
                    mgr.getAppContext(),
                    mgr.getAccountType(),
                    mgr.getLoginOptions(),
                    mgr.shouldLogoutWhenTokenRevoked()
            );

            try {
                // Try to build the rest client, launching the login flow if building it fails:
                ((SalesforceActivityInterface) activity).onResume(cm.peekRestClient());

                // Break out of the logout loop after peekRestClient() success:
                hasLaunchedLoginFlowFromPeekRestClientFailure = false;
                EventsObservable.get().notifyEvent(EventsObservable.EventType.RenditionComplete);
            } catch (final Exception ex) {
                if (hasLaunchedLoginFlowFromPeekRestClientFailure) {
                    // We failed to build the RestClient even after launching the login flow. Assume
                    // this is irrecoverable and just logout:
                    SalesforceSDKLogger.i(TAG, "Failed second attempt to get the RestClient; now cleaning up and logging out.");
                    mgr.logout(activity);
                    return;
                }

                SalesforceSDKLogger.i(TAG, "Failed first attempt to get the RestClient; launching login flow.");
                cm.launchLoginFlow(activity);
                hasLaunchedLoginFlowFromPeekRestClientFailure = true;
            }
        } else {
            ((SalesforceActivityInterface) activity).onResume(null);
        }
    }

    public void onPause() {
    }

    public void onDestroy() {
        activity.unregisterReceiver(userSwitchReceiver);
        activity.unregisterReceiver(logoutCompleteReceiver);
    }

    public boolean onKeyUp(int keyCode, KeyEvent event) {
        if (SalesforceSDKManager.getInstance().isDevSupportEnabled()) {
            if (keyCode == KeyEvent.KEYCODE_MENU) {
                SalesforceSDKManager.getInstance().showDevSupportDialog(activity);
                return true;
            }
        }
        return false;
    }


    /**
     * Acts on the user switch event.
     */
    private class ActivityUserSwitchReceiver extends UserSwitchReceiver {

        @Override
        protected void onUserSwitch() {
            ((SalesforceActivityInterface) activity).onUserSwitched();
        }
    }

    /**
     * Acts on the logout complete event.
     */
    private class ActivityLogoutCompleteReceiver extends LogoutCompleteReceiver {

        @Override
        protected void onLogoutComplete() {
            ((SalesforceActivityInterface) activity).onLogoutComplete();
        }
    }

    private static final String TAG = SalesforceActivityDelegate.class.getSimpleName();
    private static final String KEY_SAVED_STATE_PROVIDER = SalesforceActivityDelegate.class.getName();
    private static final String KEY_HAS_STARTED_LOGIN_FLOW = SalesforceActivityDelegate.class.getName() + ".KEY_HAS_STARTED_LOGIN_FLOW";
}
