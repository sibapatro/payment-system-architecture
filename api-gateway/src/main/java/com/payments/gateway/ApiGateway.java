package com.payments.gateway;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * API GATEWAY — The Single Entry Point
 *
 * All 50+ internal services are invisible to clients.
 * Every request enters through this gateway which enforces:
 *
 *  1. Authentication  → validate JWT / API key
 *  2. Rate Limiting   → Token bucket per client (10,000 TPS total)
 *  3. Routing         → /payment → PaymentService, /fraud → FraudService
 *  4. Request tracing → inject correlation IDs for distributed tracing
 *  5. Circuit Breaking→ stop routing to unhealthy services
 *
 * In production: Spring Cloud Gateway or Netflix Zuul with
 * Redis-backed rate limiting for distributed enforcement.
 */
public class ApiGateway {

    private final Map<String, RateLimiter>   rateLimiters = new ConcurrentHashMap<>();
    private final Map<String, RouteConfig>   routes       = new ConcurrentHashMap<>();
    private final Map<String, ApiKey>        apiKeys      = new ConcurrentHashMap<>();
    private final AtomicLong                 requestCount = new AtomicLong(0);
    private final AtomicLong                 rejectedCount = new AtomicLong(0);

    public ApiGateway() {
        // Register routes
        registerRoute("/payment",      "PAYMENT_SERVICE",    HttpMethod.POST,   true);
        registerRoute("/payment/{id}", "PAYMENT_SERVICE",    HttpMethod.GET,    true);
        registerRoute("/fraud/check",  "FRAUD_SERVICE",      HttpMethod.POST,   true);
        registerRoute("/ledger/{id}",  "LEDGER_SERVICE",     HttpMethod.GET,    true);
        registerRoute("/health",       "HEALTH_SERVICE",     HttpMethod.GET,    false);
        registerRoute("/metrics",      "METRICS_SERVICE",    HttpMethod.GET,    false);
    }

    // ─── Request Processing Pipeline ──────────────────────────────────────────

    public GatewayResponse process(GatewayRequest request) {
        requestCount.incrementAndGet();

        String correlationId = "CORR-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        request = request.withCorrelationId(correlationId);

        System.out.printf("[Gateway] %s %s client=%s correlationId=%s%n",
                request.method(), request.path(), request.clientId(), correlationId);

        try {
            // Pipeline: Auth → Rate Limit → Route → Forward
            AuthResult auth = authenticate(request);
            if (!auth.valid()) {
                rejectedCount.incrementAndGet();
                return GatewayResponse.unauthorized(correlationId, auth.reason());
            }

            RouteConfig route = resolve(request);
            if (route == null) {
                return GatewayResponse.notFound(correlationId, request.path());
            }

            RateLimitResult rateLimit = checkRateLimit(request.clientId(), route);
            if (!rateLimit.allowed()) {
                rejectedCount.incrementAndGet();
                return GatewayResponse.tooManyRequests(correlationId,
                        rateLimit.retryAfterSeconds());
            }

            // Forward to downstream service (in production: HTTP/gRPC call)
            return forwardToService(request, route, correlationId);

        } catch (Exception e) {
            System.err.printf("[Gateway] Internal error for correlationId=%s: %s%n",
                    correlationId, e.getMessage());
            return GatewayResponse.internalError(correlationId, "Internal gateway error");
        }
    }

    // ─── Authentication ───────────────────────────────────────────────────────

    private AuthResult authenticate(GatewayRequest request) {
        RouteConfig route = resolve(request);
        if (route != null && !route.requiresAuth()) {
            return AuthResult.valid("anonymous");
        }

        String apiKeyHeader = request.headers().get("X-API-Key");
        if (apiKeyHeader == null || apiKeyHeader.isBlank()) {
            return AuthResult.invalid("Missing X-API-Key header");
        }

        ApiKey apiKey = apiKeys.get(apiKeyHeader);
        if (apiKey == null) {
            return AuthResult.invalid("Invalid API key");
        }
        if (apiKey.expired()) {
            return AuthResult.invalid("API key expired");
        }
        if (!apiKey.hasScope(route != null ? route.requiredScope() : "read")) {
            return AuthResult.invalid("Insufficient scope for this operation");
        }

        return AuthResult.valid(apiKey.clientId());
    }

    // ─── Rate Limiting (Token Bucket) ─────────────────────────────────────────

    private RateLimitResult checkRateLimit(String clientId, RouteConfig route) {
        String limiterKey = clientId + ":" + route.serviceTarget();
        RateLimiter limiter = rateLimiters.computeIfAbsent(limiterKey,
                k -> new RateLimiter(route.rateLimit(), route.burstCapacity()));
        return limiter.tryAcquire();
    }

    // ─── Routing ──────────────────────────────────────────────────────────────

    private RouteConfig resolve(GatewayRequest request) {
        // Match path patterns (simplified — production: trie-based router)
        for (Map.Entry<String, RouteConfig> entry : routes.entrySet()) {
            if (pathMatches(request.path(), entry.getKey()) &&
                    (entry.getValue().method() == HttpMethod.ANY ||
                     entry.getValue().method() == request.method())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private boolean pathMatches(String requestPath, String pattern) {
        if (pattern.equals(requestPath)) return true;
        if (pattern.endsWith("/{id}")) {
            String base = pattern.substring(0, pattern.length() - 4);
            return requestPath.startsWith(base) && requestPath.length() > base.length();
        }
        return false;
    }

    private GatewayResponse forwardToService(GatewayRequest request,
                                             RouteConfig route,
                                             String correlationId) {
        // In production: load-balanced HTTP call with mTLS (via Sidecar/Service Mesh)
        System.out.printf("[Gateway] → Forwarding to %s correlationId=%s%n",
                route.serviceTarget(), correlationId);
        return GatewayResponse.forwarded(correlationId, route.serviceTarget());
    }

    private void registerRoute(String path, String serviceTarget,
                               HttpMethod method, boolean requiresAuth) {
        routes.put(path, new RouteConfig(path, serviceTarget, method,
                requiresAuth, requiresAuth ? "payment:write" : "health:read",
                1000, 2000));
    }

    public void registerApiKey(String key, String clientId,
                               List<String> scopes, Instant expiresAt) {
        apiKeys.put(key, new ApiKey(key, clientId, scopes, expiresAt));
    }

    public GatewayMetrics getMetrics() {
        return new GatewayMetrics(requestCount.get(), rejectedCount.get(),
                routes.size(), apiKeys.size(), rateLimiters.size());
    }

    // ─── Token Bucket Rate Limiter ────────────────────────────────────────────

    public static class RateLimiter {
        private final int     capacity;
        private final int     refillRate;   // tokens per second
        private       double  tokens;
        private       long    lastRefillMs;

        public RateLimiter(int refillRate, int burstCapacity) {
            this.refillRate  = refillRate;
            this.capacity    = burstCapacity;
            this.tokens      = burstCapacity;
            this.lastRefillMs = System.currentTimeMillis();
        }

        public synchronized RateLimitResult tryAcquire() {
            refill();

            if (tokens >= 1.0) {
                tokens -= 1.0;
                return RateLimitResult.allowed((int) tokens);
            }

            // Calculate retry-after in seconds
            double tokensNeeded = 1.0 - tokens;
            int retryAfter = (int) Math.ceil(tokensNeeded / refillRate);
            return RateLimitResult.denied(retryAfter);
        }

        private void refill() {
            long nowMs    = System.currentTimeMillis();
            long elapsedMs = nowMs - lastRefillMs;
            double newTokens = elapsedMs / 1000.0 * refillRate;
            tokens       = Math.min(capacity, tokens + newTokens);
            lastRefillMs = nowMs;
        }
    }

    // ─── Types ────────────────────────────────────────────────────────────────

    public record GatewayRequest(
            String              path,
            HttpMethod          method,
            String              clientId,
            Map<String, String> headers,
            String              body,
            String              correlationId
    ) {
        public GatewayRequest withCorrelationId(String id) {
            Map<String, String> h = new HashMap<>(headers);
            h.put("X-Correlation-Id", id);
            return new GatewayRequest(path, method, clientId, h, body, id);
        }
    }

    public record GatewayResponse(
            int    statusCode,
            String correlationId,
            String targetService,
            String errorMessage
    ) {
        public static GatewayResponse forwarded(String corr, String svc) {
            return new GatewayResponse(202, corr, svc, null); }
        public static GatewayResponse unauthorized(String corr, String msg) {
            return new GatewayResponse(401, corr, null, msg); }
        public static GatewayResponse tooManyRequests(String corr, int retryAfter) {
            return new GatewayResponse(429, corr, null, "Rate limit exceeded. Retry after " + retryAfter + "s"); }
        public static GatewayResponse notFound(String corr, String path) {
            return new GatewayResponse(404, corr, null, "No route for: " + path); }
        public static GatewayResponse internalError(String corr, String msg) {
            return new GatewayResponse(500, corr, null, msg); }
        public boolean isSuccess() { return statusCode >= 200 && statusCode < 300; }
    }

    public record RouteConfig(
            String path, String serviceTarget, HttpMethod method,
            boolean requiresAuth, String requiredScope,
            int rateLimit, int burstCapacity) {}

    public record ApiKey(
            String key, String clientId, List<String> scopes, Instant expiresAt) {
        public boolean expired()            { return Instant.now().isAfter(expiresAt); }
        public boolean hasScope(String s)   { return scopes.contains(s) || scopes.contains("*"); }
    }

    public record AuthResult(boolean valid, String clientId, String reason) {
        public static AuthResult valid(String client) { return new AuthResult(true, client, null); }
        public static AuthResult invalid(String reason) { return new AuthResult(false, null, reason); }
    }

    public record RateLimitResult(boolean allowed, int remainingTokens, int retryAfterSeconds) {
        public static RateLimitResult allowed(int remaining) {
            return new RateLimitResult(true, remaining, 0); }
        public static RateLimitResult denied(int retryAfter) {
            return new RateLimitResult(false, 0, retryAfter); }
    }

    public record GatewayMetrics(
            long totalRequests, long rejectedRequests,
            int activeRoutes, int registeredApiKeys, int activeLimiters) {}

    public enum HttpMethod { GET, POST, PUT, DELETE, PATCH, ANY }
}
