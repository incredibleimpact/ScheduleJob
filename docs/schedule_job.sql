CREATE DATABASE IF NOT EXISTS schedule_job DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

USE schedule_job;

DROP TABLE IF EXISTS sj_job_archive;
DROP TABLE IF EXISTS sj_job;

CREATE TABLE sj_job
(
    id              BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT 'primary key',
    job_name        VARCHAR(128) NOT NULL COMMENT 'job name',
    route_key       VARCHAR(64)  NOT NULL COMMENT 'route key',
    shard_key       VARCHAR(128) NOT NULL COMMENT 'shard key',
    payload_json    TEXT NULL COMMENT 'job payload',
    execute_time    DATETIME     NOT NULL COMMENT 'planned execution time',
    priority        INT          NOT NULL DEFAULT 5 COMMENT 'smaller means higher priority',
    retry_times     INT          NOT NULL DEFAULT 0 COMMENT 'retry count',
    max_retry_times INT          NOT NULL DEFAULT 3 COMMENT 'max retry count',
    creator_id      BIGINT NULL COMMENT 'creator id',
    hot_cached      TINYINT      NOT NULL DEFAULT 0 COMMENT '1-cached in redis',
    status          VARCHAR(32)  NOT NULL COMMENT 'WAITING/PAUSED/Completed/FAILED',
    claim_owner     VARCHAR(128) NULL COMMENT 'reserved runtime claim owner metadata',
    claim_time      DATETIME NULL COMMENT 'reserved runtime claim timestamp metadata',
    claim_deadline  DATETIME NULL COMMENT 'reserved runtime claim deadline metadata',
    dispatch_target VARCHAR(64) NULL COMMENT 'selected executor node',
    dispatch_note   VARCHAR(512) NULL COMMENT 'dispatch note',
    archive_path    VARCHAR(512) NULL COMMENT 'reserved for archive export path',
    create_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_job_status_hot_execute (status, hot_cached, execute_time, id),
    KEY idx_job_claim_deadline (status, claim_deadline, id),
    KEY idx_job_archive_candidate (status, update_time, id),
    KEY idx_job_creator (creator_id)
) COMMENT ='hot schedule job table';

CREATE TABLE sj_job_archive
(
    id              BIGINT PRIMARY KEY COMMENT 'original job id',
    job_name        VARCHAR(128) NOT NULL COMMENT 'job name',
    route_key       VARCHAR(64)  NOT NULL COMMENT 'route key',
    shard_key       VARCHAR(128) NOT NULL COMMENT 'shard key',
    payload_json    TEXT NULL COMMENT 'job payload',
    execute_time    DATETIME     NOT NULL COMMENT 'planned execution time',
    priority        INT          NOT NULL DEFAULT 5 COMMENT 'smaller means higher priority',
    retry_times     INT          NOT NULL DEFAULT 0 COMMENT 'retry count',
    max_retry_times INT          NOT NULL DEFAULT 3 COMMENT 'max retry count',
    creator_id      BIGINT NULL COMMENT 'creator id',
    hot_cached      TINYINT      NOT NULL DEFAULT 0 COMMENT 'always 0 after archiving',
    status          VARCHAR(32)  NOT NULL COMMENT 'ARCHIVED',
    claim_owner     VARCHAR(128) NULL COMMENT 'claim owner before archive',
    claim_time      DATETIME NULL COMMENT 'claim time before archive',
    claim_deadline  DATETIME NULL COMMENT 'claim deadline before archive',
    dispatch_target VARCHAR(64) NULL COMMENT 'executor node before archive',
    dispatch_note   VARCHAR(512) NULL COMMENT 'final dispatch note before archive',
    archive_path    VARCHAR(512) NULL COMMENT 'archive file path',
    create_time     DATETIME     NOT NULL COMMENT 'original create time',
    update_time     DATETIME     NOT NULL COMMENT 'original last update time',
    KEY idx_job_archive_path (archive_path, update_time, id),
    KEY idx_job_archive_creator (creator_id),
    KEY idx_job_archive_execute (execute_time, id)
) COMMENT ='cold schedule job archive table';
