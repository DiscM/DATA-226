-- Selects session-to-channel mappings from the demo seed CSV.
-- For production, replace ref('demo_user_session_channel') with
-- source('your_source', 'USER_SESSION_CHANNEL').

select
    "sessionId"  as "sessionId",
    "userId"     as "userId",
    "channel"    as "channel"
from {{ ref('demo_user_session_channel') }}
