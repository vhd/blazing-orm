<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Metadata;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS)]
class Record
{

    /**
     * @param string $table
     * @param array<string, Field> $fields
     * @param array<string, Index> $indexes
     * @param bool $isReference
     * @param string|null $referenceField
     * @param Index|null $primaryKey
     * @param int|null $fetchBatchLimit
     * @param int|null $insertBatchLimit
     * @param int|null $deleteBatchLimit
     * @param class-string|null $repository
     */
    public function __construct(
        public string $table,
        public array $fields = [],
        public array $indexes = [],
        public bool $isReference = false,
        public ?string $referenceField = null,
        public ?Index $primaryKey = null,
        public ?int $fetchBatchLimit = null,
        public ?int $insertBatchLimit = null,
        public ?int $deleteBatchLimit = null,
        public ?string $repository = null,
    ) {
    }

}