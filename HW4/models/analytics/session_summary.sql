-- Analytics: join session channel with timestamps to produce a summary row
-- per session, including duration in seconds.

select
    ch."sessionId"                                          as "sessionId",
    ch."userId"                                             as "userId",
    ch."channel"                                            as "channel",
    ts."startTimestamp"                                     as "startTimestamp",
    ts."endTimestamp"                                       as "endTimestamp",
    datediff('second', ts."startTimestamp", ts."endTimestamp") as "durationSeconds"
from {{ ref('user_session_channel') }}  as ch
join {{ ref('session_timestamp') }}     as ts
  on ch."sessionId" = ts."sessionId"
