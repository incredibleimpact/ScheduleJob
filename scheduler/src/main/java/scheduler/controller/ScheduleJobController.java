package scheduler.controller;


import common.CreateJobRequest;
import common.JobExecutionCallbackRequest;
import common.R;
import lombok.RequiredArgsConstructor;
import scheduler.service.ScheduleJobService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller for scheduled job endpoints.
 */
@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
public class ScheduleJobController {
    private static final String HEADER_USER_ID = "X-User-Id";

    private final ScheduleJobService scheduleJobService;

    @PostMapping
    public R create(@RequestBody CreateJobRequest request,
                    @RequestHeader(name = HEADER_USER_ID, required = false) Long creatorId) {
        try {
            return scheduleJobService.createJob(request, creatorId);
        } catch (IllegalArgumentException e) {
            return R.error(400, e.getMessage());
        }
    }

    @GetMapping("/{id}")
    public R detail(@PathVariable Long id) {
        return R.ok("job detail", scheduleJobService.getJob(id));
    }

    @GetMapping
    public R list(@RequestParam(required = false) String status) {
        return R.ok("job list", scheduleJobService.listJobs(status));
    }

    @GetMapping("/dashboard")
    public R dashboard() {
        return R.ok("dashboard", scheduleJobService.dashboard());
    }

    @PostMapping("/preheat")
    public R preheat() {
        return R.ok("preheat success", scheduleJobService.preheatHotJobs());
    }

    @PostMapping("/dispatch")
    public R dispatch() {
        return R.ok("dispatch success", scheduleJobService.dispatchDueJobs());
    }

    @PostMapping("/{id}/pause")
    public R pause(@PathVariable Long id) {
        try {
            return scheduleJobService.pauseJob(id);
        } catch (IllegalArgumentException e) {
            return R.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/resume")
    public R resume(@PathVariable Long id) {
        try {
            return scheduleJobService.resumeJob(id);
        } catch (IllegalArgumentException e) {
            return R.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/complete")
    public R complete(@PathVariable Long id, @RequestBody JobExecutionCallbackRequest request) {
        try {
            return scheduleJobService.completeJob(id, request);
        } catch (IllegalArgumentException e) {
            return R.error(400, e.getMessage());
        }
    }
}
