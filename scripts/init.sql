CREATE DATABASE IF NOT EXISTS client_tickets;
USE client_tickets;
CREATE TABLE IF NOT EXISTS client_tickets (
  ticket_id VARCHAR(64) PRIMARY KEY,
  client_id VARCHAR(64),
  created_at VARCHAR(64),
  request TEXT,
  request_type VARCHAR(64),
  priority VARCHAR(32)
);