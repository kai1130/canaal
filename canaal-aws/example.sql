CREATE OR REPLACE STREAM "DESTINATION_SQL_STREAM" (time_interval TIMESTAMP, 
                                                  chain VARCHAR(8),
                                                  symbol VARCHAR(8),
                                                  sum_amount FLOAT);
CREATE OR REPLACE PUMP "STREAM_PUMP" AS 
  INSERT INTO "DESTINATION_SQL_STREAM"
      SELECT STREAM STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '10' SECOND) as time_interval,
                    "chain",
                    "symbol",
                    SUM("amount") AS sum_amount
      FROM   "SOURCE_SQL_STREAM_001"
      GROUP BY "chain", "symbol", STEP("SOURCE_SQL_STREAM_001".ROWTIME BY INTERVAL '10' SECOND);
