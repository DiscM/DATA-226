{% snapshot snapshot_session_summary %}

{{
    config(
        unique_key    = '"sessionId"',
        strategy      = 'check',
        check_cols    = ['"userId"', '"channel"', '"startTimestamp"', '"endTimestamp"', '"durationSeconds"']
    )
}}

select * from {{ ref('session_summary') }}

{% endsnapshot %}
