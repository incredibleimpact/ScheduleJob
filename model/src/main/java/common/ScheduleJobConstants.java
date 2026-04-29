package common;

/**
 * Constants for schedule job processing.
 */
public final class ScheduleJobConstants {
    public static final String REDIS_JOB_KEY_PREFIX = "schedulejob";
    public static final String REDIS_SCHEDULER_MEMBER_KEY = "schedulejob:scheduler:members";
    public static final String REDIS_SHARD_LEASE_PREFIX = "schedulejob:lease:";
    public static final String REDIS_EXECUTOR_LOAD_KEY = "schedulejob:{executor}:load";
    public static final String REDIS_EXECUTOR_ALIVE_KEY = "schedulejob:{executor}:alive";
    public static final String REDIS_EXECUTOR_LOAD_STEP_KEY = "schedulejob:{executor}:loadstep";
    public static final String REDIS_COMMAND_STREAM_KEY = "schedulejob:stream:commands";

    private ScheduleJobConstants() {
    }

    public static String queueKey(int shard) {
        return readyKey(shard);
    }

    public static String readyKey(int shard) {
        return REDIS_JOB_KEY_PREFIX + ":{" + shard + "}:ready";
    }

    public static String inflightKey(int shard) {
        return REDIS_JOB_KEY_PREFIX + ":{" + shard + "}:inflight";
    }

    public static String detailKey(int shard) {
        return REDIS_JOB_KEY_PREFIX + ":{" + shard + "}:detail";
    }

    public static String leaseKey(int shard) {
        return REDIS_SHARD_LEASE_PREFIX + shard;
    }
}
