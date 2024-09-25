<?php

declare(strict_types=1);

namespace vhd\BlazingOrm\Utility;

use DateTimeImmutable;
use DateTimeInterface;

class DateUtil
{

    protected static DateTimeImmutable $minInstance;

    public static function minDate(): DateTimeImmutable
    {
        return self::$minInstance ??= new DateTimeImmutable('0001-01-01 00:00:00');
    }

    public static function isMinDate(DateTimeInterface $dateTime): bool
    {
        return $dateTime == self::minDate();
    }

}