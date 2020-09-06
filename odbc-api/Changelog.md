# Changelog

## 0.2.2

* Add trait `buffers::FixSizedCType`.

## 0.2.1

* Fix: `buffers::TextColumn` now correctly adjusts the target length by +1 for terminating zero.

## 0.2.0

Adds `buffers::TextRowSet`.

## 0.1.2

* Adds `col_display_size`, `col_data_type` and `col_octet_length` to `Cursor`.

## 0.1.1

* Adds convinience methods, which take UTF-8 in public interface.
* Adds `ColmunDescription::could_be_null`.
* Adds `Cursor::is_unsigned`.

## 0.1.0

Initial release
