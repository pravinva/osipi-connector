-- Grant SELECT permissions on all bronze tables to account users
-- Run this after DLT pipeline creates the tables

-- Grant on timeseries table
GRANT SELECT ON TABLE osipi.bronze.pi_timeseries TO `account users`;

-- Grant on AF hierarchy table
GRANT SELECT ON TABLE osipi.bronze.pi_af_hierarchy TO `account users`;

-- Grant on event frames table
GRANT SELECT ON TABLE osipi.bronze.pi_event_frames TO `account users`;

-- Verify permissions
SHOW GRANTS ON TABLE osipi.bronze.pi_timeseries;
SHOW GRANTS ON TABLE osipi.bronze.pi_af_hierarchy;
SHOW GRANTS ON TABLE osipi.bronze.pi_event_frames;
