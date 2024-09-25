<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Metadata;

class Column
{

    /**
     * @param string $name
     * @param string|null $ddl
     * @param mixed|null $defaultValue
     * @param array<string|class-string>|null $storedTypes
     */
    public function __construct(
        public string $name,
        public ?string $ddl = null,
        public mixed $defaultValue = null,
        public ?array $storedTypes = null,
    ) {
    }

}