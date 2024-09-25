<?php

declare(strict_types=1);

namespace vhd\BlazingOrm;

interface EventListenerInterface
{

    public function beforeFlush(RecordManagerInterface $recordManager, SyncData $data): void;

    public function onFlush(RecordManagerInterface $recordManager, SyncData $data): void;

    public function afterFlush(RecordManagerInterface $recordManager, SyncData $data): void;

}