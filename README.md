Metric Collective
=================

Metric Collective is a Windows service that polls system resources at a set
interval and forwards the collected metrics to a Graphite(Carbon) server.

Requirements
============

Metric Collective requires the following software:

* [Python 2.7.5 for Windows](http://www.python.org/download/releases/2.7.5/)
* [psutil](http://code.google.com/p/psutil/)
* [PyWin32](http://sourceforge.net/projects/pywin32/)

Installation
============

metcollect.py runs as a Windows Serivce and therefore must be registered
with Windows Services before it can be started.  Start by placing metcollect.py
and metcollect.ini in a folder on the C: drive such as C:\metcollect\

Then open a command line prompt with __Run as administator__

Register Metric Collective as a service:

    > cd c:\metcollect\
    > metcollect.py install

Edit the metcollect.ini with a text editor:

    > notepad metcollect.ini

Start Metric Collective from the command line or from services.msc:

    > metcollect.py start


Release Notes
=============

* [Issue 1](https://github.com/dividehex/metric-collective/issues/1): Convert fqdn to lowercase


1.0.0
-----

* Initial Release