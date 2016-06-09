NASDAQ TAQ
==========

NASDAQ TAQ is an application that extracts the best bids and offers (BBOs) and
trades from historical market data from NASDAQ.


Features
--------

NASDAQ TAQ supports the following file formats:

- **NASDAQ TotalView-ITCH 5.0**

NASDAQ TAQ uses [Juncture][] and [Nassau][] for NASDAQ file format support.

  [Juncture]: https://github.com/paritytrading/juncture
  [Nassau]:   https://github.com/paritytrading/nassau


Usage
-----

Run NASDAQ TAQ with Java:

    java -jar <executable> <filename> <date> <instrument>

The application displays the market events on the standard output formatted as
[TAQ][].

  [TAQ]: https://github.com/paritytrading/parity/blob/master/parity-file/doc/TAQ.md


License
-------

NASDAQ TAQ is released under the Apache License, Version 2.0.
