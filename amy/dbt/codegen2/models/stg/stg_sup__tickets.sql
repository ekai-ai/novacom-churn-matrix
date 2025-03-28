WITH source AS (
    SELECT
        *
    FROM
        {{ source(
            'raw',
            'SUP_TICKETS'
        ) }}
),
renamed AS (
    SELECT
        "TICKET_ID",
        "CUSTOMER_ID",
        "CREATED_DATE",
        "STATUS",
        "PRIORITY",
        "SUBJECT",
        "DESCRIPTION",
        "CHANNEL_OF_CONTACT",
        "ASSIGNED_AGENT",
        "RESOLUTION_DATE",
        "CREATED_AT",
        "UPDATED_AT"
    FROM
        source
)
SELECT
    *
FROM
    renamed