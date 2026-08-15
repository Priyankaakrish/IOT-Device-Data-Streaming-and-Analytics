-- =====================================================================
-- Sample seed data: machine master + planned production time
-- Run after 01_schema.sql / 02_kpi_views.sql. Sensor readings and KPI
-- aggregates are populated live by the Spark streaming job -- do not
-- seed those tables manually.
-- =====================================================================

INSERT INTO machines (machine_id, machine_name, machine_type, location, ideal_cycle_time_sec, installed_date)
VALUES
    ('M-101', 'CNC Mill 1',        'CNC',       'Plant A - Line 1', 12.0, '2022-03-01'),
    ('M-102', 'CNC Mill 2',        'CNC',       'Plant A - Line 1', 12.0, '2022-03-01'),
    ('M-103', 'Conveyor Belt A',   'Conveyor',  'Plant A - Line 1',  4.0, '2021-11-15'),
    ('M-104', 'Hydraulic Press 1', 'Press',     'Plant A - Line 2', 20.0, '2023-01-10'),
    ('M-105', 'Packaging Robot 1', 'Robot',     'Plant A - Line 2',  6.5, '2023-06-20')
ON CONFLICT (machine_id) DO NOTHING;

-- Planned production time: 8-hour shift (480 min) for today, for every machine.
INSERT INTO planned_production_time (machine_id, production_date, planned_minutes)
SELECT machine_id, CURRENT_DATE, 480 FROM machines
ON CONFLICT (machine_id, production_date) DO NOTHING;
