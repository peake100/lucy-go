CREATE TABLE IF NOT EXISTS jobs_batches (
    -- id should be a uuid in 16-byte format
    id blob(16)
        primary key
        not null
        check (length(id) == 16),

    created datetime
        not null
        default (strftime('%Y-%m-%d %H:%M:%f', 'now')),

    modified datetime
        not null
        default (strftime('%Y-%m-%d %H:%M:%f', 'now')),

    type text
        not null,

    name text
        not null,

    description text
        not null,

    progress float
        not null
        default 0
        check ( 0.0 <= progress <= 1.0 ),

    job_count text
        not null
        default 0
        check ( job_count >= 0 ),

    pending_count integer
        not null
        default 0
        check ( pending_count >= 0 ),

    cancelled_count integer
        not null
        default 0
        check ( cancelled_count >= 0 ),

    running_count integer
        not null
        default 0
        check ( running_count >= 0 ),

    completed_count integer
        not null
        default 0
        check ( completed_count >= 0 ),

    success_count integer
        not null
        default 0
        check ( success_count >= 0 ),

    failure_count integer
        not null
        default 0
        check ( failure_count >= 0 )
);

CREATE TABLE IF NOT EXISTS jobs_jobs (
    -- id should be a uuid in 16-byte format
    id blob(16)
        primary key
        not null
        check (length(id) == 16),

    -- batch_id should be a uuid in 16-byte format
    batch_id blob(16)
        not null
        check (length(batch_id) == 16),

    created datetime
        not null
        default (strftime('%Y-%m-%d %H:%M:%f', 'now')),

    modified datetime
        not null
        default (strftime('%Y-%m-%d %H:%M:%f', 'now')),

    type text
        not null,

    name text
        not null,

    description text
        not null,

    -- input will be a protobuf-encoded message, and will not be searchable /
    -- inspectable until decoded by the application layer.
    input blob,

    run_count integer
        not null
        default 0
        check ( run_count >= 0 )
        check ( run_count <= max_retries + 1 ),

    max_retries integer
        not null
        default 0
        check ( max_retries >= 0 ),

    progress float
        not null
        default 0
        check ( 0.0 <= progress <= 1.0 ),

    status text
        not null
        default 'PENDING'
        check ( status in ('PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED') ),

    result text
        not null
        default 'NONE'
        check ( result in ('NONE', 'SUCCEEDED', 'FAILED') ),

    -- hook the batch_id field up to the batch record.
    foreign key (batch_id)
        references jobs_batches(id)

);

CREATE TABLE IF NOT EXISTS jobs_stages (
    -- stage index is the stage number for the job.
    stage_index
        integer
        not null
        check (stage_index >= 0),

    -- job_id should be a uuid in 16-byte format
    job_id blob(16)
        not null
        check (length(job_id) == 16),

    created datetime
        not null
        default (strftime('%Y-%m-%d %H:%M:%f', 'now')),

    modified datetime
        not null
        default (strftime('%Y-%m-%d %H:%M:%f', 'now')),

    type text
        not null
        check (type != ''),

    description text
        not null
        check (description != ''),

    status text
        not null
        default 'PENDING'
        check ( status in ('PENDING', 'RUNNING', 'COMPLETED', 'CANCELLED') ),

    run_count integer
        not null
        default 0
        check ( run_count >= 0 ),

    run_by text
        not null
        default '',

    started datetime
        default NULL,

    completed datetime
        default NULL,

    progress float
        not null
        default 0
        check ( 0.0 <= progress <= 1.0 ),

    result text
        not null
        default 'NONE'
        check ( result in ('NONE', 'SUCCEEDED', 'FAILED') ),

    -- result_data is a protobuf-encoded data object for application consumption.
    result_data blob
        default NULL,

    -- error is a protobuf-encoded data object for application consumption.
    error blob
        default NULL,

    primary key (job_id, stage_index)

    -- hook the job_id field up to the job record.
    foreign key (job_id)
        references jobs_jobs(id)
);

-- This trigger updates the progress value of a job whenever one of it's stage's
-- progress values are set.
CREATE TRIGGER IF NOT EXISTS job_added
    AFTER INSERT ON jobs_jobs
    BEGIN
        UPDATE jobs_batches
        SET
            job_count = (
                SELECT count(*) FROM jobs_jobs WHERE batch_id == NEW.batch_id
            ),
            pending_count = (
                SELECT count(*) FROM jobs_jobs
                WHERE batch_id == NEW.batch_id AND status == 'PENDING'
            )
        WHERE id == NEW.batch_id;
    END;

CREATE TRIGGER IF NOT EXISTS batches_update_modified
    AFTER UPDATE ON jobs_batches
    BEGIN
        UPDATE jobs_batches
            SET modified = (strftime('%Y-%m-%d %H:%M:%f', 'now'))
            WHERE id == NEW.id;
    END;

CREATE TRIGGER IF NOT EXISTS jobs_update_modified
    AFTER UPDATE ON jobs_jobs
    BEGIN
        UPDATE jobs_jobs
            SET modified = (strftime('%Y-%m-%d %H:%M:%f', 'now'))
            WHERE id == NEW.id;
    END;

-- This trigger updates the progress value of a job whenever one of it's stage's
-- progress values are set.
CREATE TRIGGER IF NOT EXISTS stage_progress_updated
    UPDATE OF progress ON jobs_stages

    BEGIN
        UPDATE jobs_jobs
        SET progress=max(
            1.0, (SELECT avg(progress) FROM jobs_stages WHERE id==NEW.batch_id)
        )
        WHERE id == NEW.job_id;
    END;

CREATE TRIGGER IF NOT EXISTS stage_status_updaded
    UPDATE OF status ON jobs_stages

    BEGIN
        UPDATE jobs_jobs
        SET
            status = (
                CASE
                    WHEN (SELECT count(*) FROM jobs_stages
                        WHERE job_id == NEW.job_id
                            AND status == 'CANCELLED' LIMIT 1) == 1
                        THEN 'CANCELLED'
                    WHEN (SELECT count(*) FROM jobs_stages
                        WHERE job_id == NEW.job_id
                            AND result == 'FAILED' LIMIT 1) == 1
                        THEN 'COMPLETED'
                    WHEN (SELECT count(*) FROM jobs_stages
                        WHERE job_id == NEW.job_id
                            AND status == 'RUNNING' LIMIT 1) == 1
                        THEN 'RUNNING'
                    ELSE 'PENDING'
                END
            )
        WHERE id == NEW.job_id;
    END;

-- This trigger updates the progress value of a job whenever one of it's stage's
-- progress values are set.
CREATE TRIGGER IF NOT EXISTS job_progress_updated
    UPDATE OF progress ON jobs_jobs

    BEGIN
        UPDATE jobs_batch
        SET progress=max(
            1.0, (SELECT avg(progress) FROM jobs_jobs WHERE id=NEW.batch_id)
        )
        WHERE id == NEW.batch_id;
    END
