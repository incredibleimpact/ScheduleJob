package common;

/**
 * Supported scheduler commands delivered through Redis Stream.
 */
public enum JobCommandType {
    CREATE_JOB,
    PAUSE_JOB,
    RESUME_JOB,
    COMPLETE_JOB
}
