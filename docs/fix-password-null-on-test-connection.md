# Fix: password becomes `null` when testing a new DB connection

## Symptom
Testing a brand-new (not-yet-saved) database connection — or any request to the
standalone test-connection endpoint — fails with a null-password error, even
though the password was correctly typed/sent in the request.

## Root cause
`HttpSecretMaskingConfig.java` registers a Jackson mixin that puts `@JsonIgnore`
on the password **getter only**, intending to hide passwords from HTTP
responses while still letting clients submit a new password in requests.

That assumption is wrong. In Jackson, `@JsonIgnore` on *either* accessor
(getter or setter) marks the whole logical property as ignored for **both**
serialization and deserialization — not just the direction it's placed on —
unless another accessor for the same property explicitly re-enables it. With
no such override, every incoming JSON body carrying a real `password` field
silently deserialized to `null` on the server side, regardless of what the
client actually sent.

## Fix
Add an explicit `@JsonProperty("password")` on the **setter** in the mixin.
This re-enables deserialization only — serialization stays suppressed, so
passwords still never leak back out in any HTTP response.

```java
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

abstract static class ConnectionConfigMixin {
    @JsonIgnore
    abstract String getPassword();
    @JsonProperty("password")           // <-- the fix
    abstract void setPassword(String password);
}
```

Apply the identical change to any other mixin with the same
getter-only-`@JsonIgnore` pattern on a password-like field (in this codebase,
`DbUserConfigMixin` had the exact same bug).

## How to verify
1. Rebuild and restart.
2. `POST` a real connection test with a real password:
   ```bash
   curl -X POST http://localhost:PORT/api/plsql/config/test-connection \
     -H "Content-Type: application/json" \
     -d '{"name":"x","username":"user","password":"realpass","hostname":"host","port":1521,"serviceName":"SVC","connectionType":"SERVICE_NAME","dbType":"ORACLE"}'
   ```
3. Before the fix: a null-password-shaped failure regardless of DB reachability.
   After the fix: a real connection result — either `"success":true` with a
   real DB version string, or a real driver error (wrong host/service/etc.),
   proving the password actually reached the JDBC driver.
4. Confirm passwords still never appear in any `GET` response for existing
   connections — the serialization-side suppression is unaffected.

## General lesson
`@JsonIgnore` on one accessor of a getter/setter pair is not a "hide on read,
allow on write" toggle — it's an all-or-nothing switch for that property
unless you explicitly split the two directions with `@JsonProperty` on the
accessor you want to keep active. Any codebase doing "hide sensitive field
from responses but still accept it in requests" via a bare `@JsonIgnore` on
just the getter has this same latent bug.
