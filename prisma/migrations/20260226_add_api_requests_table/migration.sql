-- API usage tracking table for operational dashboards
CREATE TABLE api_requests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    method VARCHAR(10) NOT NULL,
    path VARCHAR(500) NOT NULL,
    route_path VARCHAR(500),
    status_code INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    client_ip VARCHAR(45),
    user_agent VARCHAR(500),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_requests_created_at ON api_requests(created_at);
CREATE INDEX idx_api_requests_path ON api_requests(path);
CREATE INDEX idx_api_requests_route_path ON api_requests(route_path);
CREATE INDEX idx_api_requests_status_code ON api_requests(status_code);
