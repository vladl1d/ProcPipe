# -*- coding: utf-8 -*-
"""
Created on Wed Nov 14 19:22:37 2018

@author: V-Liderman
"""
import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

def parse_iso_datetime_format(cls, date_string):
    """Construct a datetime from the output of datetime.isoformat()."""
    def _parse_hh_mm_ss_ff(tstr):
        # Parses things of the form HH[:MM[:SS[.fff[fff]]]]
        len_str = len(tstr)

        time_comps = [0, 0, 0, 0]
        pos = 0
        for comp in range(0, 3):
            if (len_str - pos) < 2:
                raise ValueError('Incomplete time component')

            time_comps[comp] = int(tstr[pos:pos+2])

            pos += 2
            next_char = tstr[pos:pos+1]

            if not next_char or comp >= 2:
                break

            if next_char != ':':
                raise ValueError('Invalid time separator: %c' % next_char)

            pos += 1

        if pos < len_str:
            if tstr[pos] != '.':
                raise ValueError('Invalid microsecond component')
            else:
                pos += 1

                len_remainder = len_str - pos
                if len_remainder not in (3, 6):
                    raise ValueError('Invalid microsecond component')

                time_comps[3] = int(tstr[pos:])
                if len_remainder == 3:
                    time_comps[3] *= 1000

        return time_comps
    def _parse_isoformat_date(dtstr):
        # It is assumed that this function will only be called with a
        # string of length exactly 10, and (though this is not used) ASCII-only
        year = int(dtstr[0:4])
        if dtstr[4] != '-':
            raise ValueError('Invalid date separator: %s' % dtstr[4])

        month = int(dtstr[5:7])

        if dtstr[7] != '-':
            raise ValueError('Invalid date separator')

        day = int(dtstr[8:10])

        return [year, month, day]
    def _parse_isoformat_time(tstr):
        # Format supported is HH[:MM[:SS[.fff[fff]]]][+HH:MM[:SS[.ffffff]]]
        len_str = len(tstr)
        if len_str < 2:
            raise ValueError('Isoformat time too short')

        # This is equivalent to re.search('[+-]', tstr), but faster
        tz_pos = (tstr.find('-') + 1 or tstr.find('+') + 1)
        timestr = tstr[:tz_pos-1] if tz_pos > 0 else tstr

        time_comps = _parse_hh_mm_ss_ff(timestr)

        tzi = None
        if tz_pos > 0:
            tzstr = tstr[tz_pos:]

            # Valid time zone strings are:
            # HH:MM               len: 5
            # HH:MM:SS            len: 8
            # HH:MM:SS.ffffff     len: 15

            if len(tzstr) not in (5, 8, 15):
                raise ValueError('Malformed time zone string')

            tz_comps = _parse_hh_mm_ss_ff(tzstr)
            if all(x == 0 for x in tz_comps):
                tzi = timezone.utc
            else:
                tzsign = -1 if tstr[tz_pos - 1] == '-' else 1

                td = timedelta(hours=tz_comps[0], minutes=tz_comps[1],
                               seconds=tz_comps[2], microseconds=tz_comps[3])

                tzi = timezone(tzsign * td)

        time_comps.append(tzi)

        return time_comps
    ################
    assert isinstance(date_string, str)
    # Split this at the separator
    dstr = date_string[0:10]
    tstr = date_string[11:]

    try:
        date_components = _parse_isoformat_date(dstr)
    except ValueError:
        raise ValueError('Invalid isoformat string: {date_string!r}')

    if tstr:
        try:
            time_components = _parse_isoformat_time(tstr)
        except ValueError:
            raise ValueError('Invalid isoformat string: {date_string!r}')
    else:
        time_components = [0, 0, 0, 0, None]

    return cls(*(date_components + time_components))

def get_typed_value(value, _type):
    '''Получение типизированного значения из строки'''
    if _type == int:
        return int(value)
    elif _type == float:
        return float(value)
    elif _type == bool:
        return value==True
    elif _type == UUID:
        return UUID(value)
    elif _type == datetime:
        if isinstance(value, str):
            return parse_iso_datetime_format(datetime, value)
        elif isinstance(value, bytes):
            return parse_iso_datetime_format(datetime, value.decode())
        elif isinstance(value, datetime):
            return value
        elif isinstance(value, float):
            return datetime.fromtimestamp(value)
        elif isinstance(value, int):
            return datetime.fromordinal(value)
        else:
            raise TypeError('Неизвестный формат времени')
    else:
        return str(value)

class CustomEncoder(json.JSONEncoder):
    '''Helper Для сериализации составных типов'''
    def default(self, value):
        #обработка дополнительных типов
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, UUID):
            return str(value)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, value)
