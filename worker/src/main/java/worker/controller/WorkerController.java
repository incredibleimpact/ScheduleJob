package worker.controller;

import common.R;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import common.WorkerExecuteRequest;
import worker.service.WorkerService;

/**
 * Controller for worker endpoints.
 */
@RestController
@RequestMapping("/worker/jobs")
@RequiredArgsConstructor
public class WorkerController {
    private final WorkerService workerService;

    @PostMapping("/execute")
    public R execute(@RequestBody WorkerExecuteRequest request) {
        try {
            return R.ok("worker accepted", workerService.execute(request));
        } catch (IllegalArgumentException e) {
            return R.error(409, e.getMessage());
        }
    }
}
