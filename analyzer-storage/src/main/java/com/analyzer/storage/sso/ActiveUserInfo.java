package com.analyzer.storage.sso;

/**
 * One row of {@link SsoSessionRegistry#listActive()}: who a live session is, where it points, and
 * when it was last seen — the admin portal's view of the connected users.
 *
 * <p>A session that has started a flow but not completed it appears here with a {@code null}
 * {@code identity}; a session that has not connected a bucket appears with {@code null}
 * {@code bucket}/{@code region}/{@code capabilities}. Consumers that want only fully-joined users
 * filter on {@code identity}, and the portal keys these rows the same way it keys audit events.
 *
 * <p>Contains no token or credential material — it is a status view, safe to serialise.
 *
 * @param sessionId          the {@code HttpSession} id
 * @param identity           who the session is, or {@code null} when the flow has not completed
 * @param bucket             the verified bucket, or {@code null} before a connect
 * @param region             the verified region, or {@code null} before a connect
 * @param capabilities       the last probe outcome, or {@code null} before a connect
 * @param lastActivityEpochMs when the registry last handed this session out
 */
public record ActiveUserInfo(String sessionId,
                             SsoIdentity identity,
                             String bucket,
                             String region,
                             SsoStoreSwapper.Capabilities capabilities,
                             long lastActivityEpochMs) {
}
