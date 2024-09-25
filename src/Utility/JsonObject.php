<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Utility;

use JsonSerializable;

class JsonObject implements JsonSerializable
{

    public function __construct(
        protected array $value = [],
    ) {
    }

    public function setValue(mixed $value, string $path = ''): void
    {
        if (!$path) {
            $this->value = $value;
            return;
        }
        $path = explode('.', $path);
        $lastIdx = array_key_last($path);
        $current = &$this->value;
        foreach ($path as $idx => $key) {
            if (!isset($current[$key])) {
                $current[$key] = [];
            }
            if ($idx === $lastIdx && $value === null) {
                unset($current[$key]);
                return;
            }
            $current = &$current[$key];
        }
        $current = $value;
    }

    public function getValue(string $path = ''): mixed
    {
        if (!$path) {
            return $this->value;
        }
        $path = explode('.', $path);
        $current = &$this->value;
        foreach ($path as $key) {
            if (!isset($current[$key])) {
                return null;
            }
            $current = &$current[$key];
        }
        return $current;
    }

    public function jsonSerialize(): mixed
    {
        return $this->value;
    }

}