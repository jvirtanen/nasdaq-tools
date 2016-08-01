Nasdaq TAQ
==========

Nasdaq TAQ is an application that extracts the best bids and offers (BBOs) and
trades from historical market data from Nasdaq.


Features
--------

Nasdaq TAQ supports the following file formats:

- **NASDAQ TotalView-ITCH 5.0**

Nasdaq TAQ uses [Juncture][] and [Nassau][] for Nasdaq file format support and
[Parity][] for order book reconstruction and TAQ file format support.

  [Juncture]: https://github.com/paritytrading/juncture
  [Nassau]:   https://github.com/paritytrading/nassau
  [Parity]:   https://github.com/paritytrading/parity


Usage
-----

Run Nasdaq TAQ with Java:

    java -jar <executable> <filename> <date> <instrument>

The application displays the market events on the standard output formatted as
[TAQ][].

  [TAQ]: https://github.com/paritytrading/parity/blob/master/parity-file/doc/TAQ.md


License
-------

Nasdaq TAQ is released under the Apache License, Version 2.0.
