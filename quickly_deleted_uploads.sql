--Documents uploaded and deleted within 24 hours with page counts
WITH creates AS (
    SELECT project_id, metadata_id, event_date_time
    FROM merrill.prod_us_dbt.audit_events_content_change_view
    WHERE event_name = 'CONTENT_UPLOAD_CREATE'),
deletes AS (
    SELECT project_id, metadata_id, event_date_time
    FROM merrill.prod_us_dbt.audit_events_content_change_view
    WHERE event_name = 'CONTENT_DELETE'),
page_counts AS (
    SELECT 
        DATA:metadataId::String AS metadata_id,
        DATA:documentPageCount::Number AS page_count
    FROM MERRILL.PROD_DS1_AUDIT_EVENTS_USA.DOCUMENT_STATE_CHANGE
    WHERE DATA:eventName::String = 'DOCUMENT_FINALIZED'
)
SELECT a.project_name, a.project_id, COUNT(*) AS counts, SUM(page_count)
FROM (
    SELECT pd.project_name, c.*, pc.page_count
    FROM creates c
    INNER JOIN deletes d ON c.metadata_id = d.metadata_id
    INNER JOIN merrill.prod_us_dbt.projects_details pd ON c.project_id = pd.id
    LEFT JOIN page_counts pc ON c.metadata_id = pc.metadata_id
    WHERE d.event_date_time <= dateadd(day, 1, c.event_date_time) AND pd.demo = false) a
GROUP BY a.project_name, a.project_id
