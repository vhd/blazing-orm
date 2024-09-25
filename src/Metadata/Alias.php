<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Metadata;

use Attribute;

#[Attribute(Attribute::TARGET_CLASS)]
class Alias
{

    public function __construct(
        public string $name,
    ) {
    }

}