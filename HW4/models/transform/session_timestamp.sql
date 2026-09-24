-- Selects session start and end timestamps from the demo seed CSV.
-- For production, replace ref('demo_session_timestamp') with
-- source('your_source', 'SESSION_TIMESTAMP').

select
    "sessionId"       as "sessionId",
    "startTimestamp"  as "startTimestamp",
    "endTimestamp"    as "endTimestamp"
from {{ ref('demo_session_timestamp') }}
