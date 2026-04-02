<?php

namespace App\Jobs\StatusPipeline;

use App\Media;
use App\Status;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Facades\Log;

class NewStatusPipeline implements ShouldQueue
{
    use Dispatchable, InteractsWithQueue, Queueable, SerializesModels;

    protected $status;

    /**
     * Delete the job if its models no longer exist.
     *
     * @var bool
     */
    public $deleteWhenMissingModels = true;

    public $timeout = 30;

    public $tries = 3;

    public $maxExceptions = 3;

    public $backoff = [5, 15, 30];

    /**
     * Create a new job instance.
     *
     * @return void
     */
    public function __construct(Status $status)
    {
        $this->status = $status;
    }

    /**
     * Execute the job.
     *
     * @return void
     */
    public function handle()
    {
        $status = $this->status;

        // Verify status exists
        if (! $status) {
            Log::info('NewStatusPipeline: Status no longer exists, skipping job');

            return;
        }

        if (! Status::where('id', $status->id)->exists()) {
            // The status has already been deleted by the time the job is running
            // Don't publish the status, and just no-op
            return;
        }
        if (config_cache('pixelfed.cloud_storage')) {
            $still_processing = Media::whereStatusId($this->status->id)
                ->whereNull('cdn_url')
                ->exists();
            if ($still_processing) {
                // Media is still being uploaded to S3. Defer federation until all
                // media have cdn_url set so federated payloads use the correct URLs.
                // MediaStorageService will re-dispatch this job once uploads complete.
                return;
            }
        }

        try {
            StatusEntityLexer::dispatch($status);
        } catch (\Exception $e) {
            Log::warning("NewStatusPipeline: Failed to dispatch StatusEntityLexer for status {$status->id}: ".$e->getMessage());
            throw $e;
        }
    }
}
