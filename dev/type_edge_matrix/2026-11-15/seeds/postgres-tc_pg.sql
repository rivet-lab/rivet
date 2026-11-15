DROP TABLE IF EXISTS tc_bqsf_pg;
CREATE TABLE tc_bqsf_pg (
  id int PRIMARY KEY,
  c_int8 bigint,
  c_num38 numeric(38,10),
  c_num50 numeric(50,10),
  c_f8 double precision,
  c_date date,
  c_ts timestamp(6),
  c_tstz timestamptz(6),
  c_time time(6),
  c_interval interval,
  c_uuid uuid,
  c_jsonb jsonb,
  c_bytea bytea,
  c_int8arr bigint[],
  c_textarr text[]
);
INSERT INTO tc_bqsf_pg VALUES
(1, -9223372036854775808, -9999999999999999999999999999.9999999999, -1234567890123456789012345678901234567890.1234567890, 'NaN', '0001-01-01', '1970-01-01 00:00:00.000001', '1970-01-01 00:00:00.000001+00', '00:00:00.000001', '1 day 2 hours 3.000004 seconds', '00000000-0000-0000-0000-000000000000', '{"a":1}', '\x00ff', '{}', '{}'),
(2, 9223372036854775807, 9999999999999999999999999999.9999999999, 9999999999999999999999999999999999999999.9999999999, 'Infinity', '9999-12-31', '0001-01-01 00:00:00', '9999-12-31 23:59:59.999999+00', '23:59:59.999999', '-1 mons', 'ffffffff-ffff-ffff-ffff-ffffffffffff', '[1,"x"]', '\x', '{9223372036854775807,-9223372036854775808}', '{a,NULL}'),
(3, 0, 0.0000000001, 0.0000000001, '-Infinity', '1969-12-31', '1969-12-31 23:59:59.999999', '1969-12-31 23:59:59.999999+00', '12:00:00', '0 seconds', '6ba7b810-9dad-11d1-80b4-00c04fd430c8', 'null', '\xdeadbeef', '{1,NULL}', NULL),
(4, 1, -0.5, 1.5, 2.2250738585072014e-308, '2000-02-29', '2000-02-29 12:34:56.789012', '2000-02-29 12:34:56.789012+00', '01:02:03.456789', '-2 years 3 months', NULL, NULL, NULL, NULL, NULL),
(5, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
