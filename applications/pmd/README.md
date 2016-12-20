Nasdaq PMD
==========

Nasdaq PMD is an application that converts historical market data from Nasdaq
to [Parity][].

  [Parity]: https://github.com/paritytrading/parity


Usage
-----

Run Nasdaq PMD with Java:

    java -jar nasdaq-pmd.jar <input-file> <output-file>

Given an input file containing NASDAQ TotalView-ITCH 5.0 messages, the
application produces an output file containing corresponding [PMD][] messages.

  [PMD]: https://github.com/paritytrading/parity/tree/master/parity-net/doc/PMD.md


License
-------

Released under the Apache License, Version 2.0.
