<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Metadata;

use Attribute;

#[Attribute(Attribute::TARGET_PROPERTY)]
class Field
{

    /**
     * @param int $length
     * @param int|null $decimalDigits
     * @param int $decimalFractionDigits
     * @param bool $primary
     * @param bool $index
     * @param string|null $name
     * @param array<string|class-string> $phpTypes
     * @param array<string, Column> $columns
     * @param string|null $typeHint
     */
    public function __construct(
        public int $length = 120,
        public ?int $decimalDigits = null,
        public int $decimalFractionDigits = 0,
        public bool $primary = false,
        public bool $index = false,
        public ?string $name = null,
        public array $phpTypes = [],
        public ?array $columns = null,
        public ?string $typeHint = null,
    ) {
    }

}