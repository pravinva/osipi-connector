-- Setup bronze tables in osipi catalog
-- Run this once to initialize the schema

-- Create bronze schema if not exists
CREATE SCHEMA IF NOT EXISTS osipi.bronze;

-- PI Timeseries Data Table
CREATE TABLE IF NOT EXISTS osipi.bronze.pi_timeseries (
    tag_webid STRING NOT NULL COMMENT 'PI Point WebID',
    tag_name STRING COMMENT 'PI Point name',
    timestamp TIMESTAMP NOT NULL COMMENT 'Data timestamp',
    value DOUBLE COMMENT 'Numeric value',
    units STRING COMMENT 'Engineering units',
    quality STRING COMMENT 'Data quality indicator',
    ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'When data was ingested',
    plant STRING COMMENT 'Plant/site identifier',
    unit INT COMMENT 'Unit number',
    sensor_type STRING COMMENT 'Type of sensor'
)
USING DELTA
PARTITIONED BY (DATE(timestamp))
COMMENT 'PI timeseries data from mock PI Web API';

-- PI AF Hierarchy Table
CREATE TABLE IF NOT EXISTS osipi.bronze.pi_af_hierarchy (
    webid STRING NOT NULL COMMENT 'Element WebID',
    name STRING COMMENT 'Element name',
    template_name STRING COMMENT 'AF template name',
    description STRING COMMENT 'Element description',
    path STRING COMMENT 'Full AF path',
    parent_webid STRING COMMENT 'Parent element WebID',
    plant STRING COMMENT 'Plant identifier',
    unit INT COMMENT 'Unit number',
    equipment_type STRING COMMENT 'Equipment type',
    ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'When data was ingested'
)
USING DELTA
COMMENT 'PI AF (Asset Framework) hierarchy from mock PI Web API';

-- PI Event Frames Table
CREATE TABLE IF NOT EXISTS osipi.bronze.pi_event_frames (
    webid STRING NOT NULL COMMENT 'Event Frame WebID',
    name STRING COMMENT 'Event Frame name',
    template_name STRING COMMENT 'Event Frame template',
    start_time TIMESTAMP NOT NULL COMMENT 'Event start time',
    end_time TIMESTAMP COMMENT 'Event end time',
    primary_element_webid STRING COMMENT 'Referenced element WebID',
    description STRING COMMENT 'Event description',
    attributes MAP<STRING, STRING> COMMENT 'Event attributes as key-value pairs',
    ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'When data was ingested'
)
USING DELTA
PARTITIONED BY (DATE(start_time))
COMMENT 'PI Event Frames (batch runs, alarms, maintenance) from mock PI Web API';

-- Create indexes for better query performance
ALTER TABLE osipi.bronze.pi_timeseries SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE osipi.bronze.pi_af_hierarchy SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

ALTER TABLE osipi.bronze.pi_event_frames SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
