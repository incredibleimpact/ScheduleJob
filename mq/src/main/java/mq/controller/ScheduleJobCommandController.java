package mq.controller;

import common.CreateJobRequest;
import common.JobExecutionCallbackRequest;
import common.R;
import lombok.RequiredArgsConstructor;
import mq.service.ScheduleJobCommandPublisher;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * HTTP ingress for scheduler write commands.
 */
@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
public class ScheduleJobCommandController {
    private static final String HEADER_USER_ID = "X-User-Id";

    private final ScheduleJobCommandPublisher scheduleJobCommandPublisher;

    @PostMapping
    public R create(@RequestBody CreateJobRequest request,
                    @RequestHeader(name = HEADER_USER_ID, required = false) Long creatorId) {
        return scheduleJobCommandPublisher.publishCreateJob(request, creatorId);
    }

    @PostMapping("/{id}/pause")
    public R pause(@PathVariable Long id) {
        return scheduleJobCommandPublisher.publishPauseJob(id);
    }

    @PostMapping("/{id}/resume")
    public R resume(@PathVariable Long id) {
        return scheduleJobCommandPublisher.publishResumeJob(id);
    }

    @PostMapping("/{id}/complete")
    public R complete(@PathVariable Long id, @RequestBody JobExecutionCallbackRequest request) {
        return scheduleJobCommandPublisher.publishCompleteJob(id, request);
    }
}
