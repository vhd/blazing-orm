<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Metadata;

class Index
{

    /**
     * @param string|null $name
     * @param array<string> $columns
     * @param string|null $ddl
     * @param array<string> $fields
     * @param bool $unique
     */
    public function __construct(
        public ?string $name = null,
        public ?array $columns = null,
        public ?string $ddl = null,
        public array $fields = [],
        public bool $unique = false,
    ) {
    }

}