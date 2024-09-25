<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Utility;

use JetBrains\PhpStorm\ExpectedValues;

class RecordState
{

    public const int STATE_REFERENCE = 0;
    public const int STATE_NEW = 1;
    public const int STATE_STORED = 2;
    public const int STATE_DELETED = 3;

    public const int TASK_NONE = 0;
    public const int TASK_FETCH = 1;
    public const int TASK_STORE = 2;
    public const int TASK_DELETE = 3;

    #[ExpectedValues(values: [
        self::STATE_REFERENCE,
        self::STATE_NEW,
        self::STATE_STORED,
        self::STATE_DELETED,
    ])]
    public int $state;

    #[ExpectedValues(values: [
        self::TASK_NONE,
        self::TASK_FETCH,
        self::TASK_STORE,
        self::TASK_DELETE,
    ])]
    public int $task;

    public ?string $key;

    public function canBePersisted(): bool
    {
        if ($this->task === self::TASK_FETCH
            || $this->task === self::TASK_DELETE
            || $this->state === self::STATE_REFERENCE
            || $this->state === self::STATE_DELETED
        ) {
            return false;
        }
        return true;
    }

    public function canBeRemoved(): bool
    {
        if ($this->task === self::TASK_STORE
            || $this->state === self::STATE_NEW
            || $this->state === self::STATE_DELETED
        ) {
            return false;
        }
        return true;
    }

}